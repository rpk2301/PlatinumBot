import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
} from "@aws-sdk/lib-dynamodb";

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const {
  USERS, // JSON string: [{"name":"Ricky","steamId":"7656..."}, ...]
  STEAM_API_KEY,
  DISCORD_WEBHOOK_URL, // default webhook if user doesn't provide one
  DDB_TABLE,           // state table (per user+game)
  TIMEZONE = "America/New_York",
  ACH_WINDOW_SECONDS = "900", // 15 minutes default
  CONCURRENCY = "3",
  PLATINUM_IMAGE_URL = "https://i.imgur.com/8mQe7pD.jpeg",

  // Guard rail (bytes). DynamoDB item limit is 400KB; keep a buffer.
  DDB_ITEM_MAX_BYTES = "350000",

  // Weekly leaderboard event logging (optional)
  EVENTS_TABLE,          // e.g. "SteamAchievementEvents"
  EVENT_TTL_DAYS = "120"
} = process.env;

const DEFAULT_WINDOW = Number(ACH_WINDOW_SECONDS) || 900;
const MAX_CONCURRENCY = Math.max(1, Number(CONCURRENCY) || 3);
const MAX_ITEM_BYTES = Math.max(100000, Number(DDB_ITEM_MAX_BYTES) || 350000);

function mustEnv(name) {
  if (!process.env[name]) throw new Error(`Missing env var: ${name}`);
}

mustEnv("USERS");
mustEnv("STEAM_API_KEY");
mustEnv("DISCORD_WEBHOOK_URL");
mustEnv("DDB_TABLE");

function parseUsers() {
  let parsed;
  try {
    parsed = JSON.parse(USERS);
  } catch {
    throw new Error(`USERS must be valid JSON. Got: ${USERS}`);
  }
  if (!Array.isArray(parsed) || parsed.length === 0) {
    throw new Error("USERS must be a non-empty JSON array.");
  }
  for (const u of parsed) {
    if (!u?.name || !u?.steamId) {
      throw new Error(`Each user must include { "name", "steamId" }. Bad entry: ${JSON.stringify(u)}`);
    }
  }
  return parsed;
}

function log(name, msg) {
  console.log(`[${name}] ${msg}`);
}

async function steamGet(name, url) {
  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.text();
    log(name, `Steam API call failed: ${res.status} ${body}`);
    throw new Error(`Steam API error ${res.status}: ${body}`);
  }
  return res.json();
}

function percent(numer, denom) {
  if (!denom) return 0;
  return Math.floor((numer / denom) * 100);
}

function formatLocalDateFromUnix(sec, tz) {
  if (!sec) return "Unknown";
  return new Date(sec * 1000).toLocaleString("en-US", { timeZone: tz });
}

/* -------------------- Leaderboard Event Logging -------------------- */

function isoWeekKey(dateUtc) {
  const d = new Date(Date.UTC(dateUtc.getUTCFullYear(), dateUtc.getUTCMonth(), dateUtc.getUTCDate()));
  const day = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - day);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const weekNo = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
  return `${d.getUTCFullYear()}-W${String(weekNo).padStart(2, "0")}`;
}

async function recordAchievementEvent({
  name,
  steamId,
  appid,
  gameTitle,
  achievementApiName,
  achievementDisplayName,
  rarityPercent,
  unlockedAtSec,
}) {
  if (!EVENTS_TABLE) return;

  const week = isoWeekKey(new Date(unlockedAtSec * 1000));
  const pk = `week#${week}`;
  const sk =
    `user#${steamId}` +
    `#t#${String(unlockedAtSec).padStart(10, "0")}` +
    `#app#${appid}` +
    `#ach#${achievementApiName}`;

  const ttlDays = Number(EVENT_TTL_DAYS) || 120;
  const ttl = Math.floor(Date.now() / 1000) + ttlDays * 86400;

  const item = {
    PK: pk,
    SK: sk,
    week,
    name,
    steamId,
    appid,
    gameTitle,
    achievementApiName,
    achievementDisplayName,
    rarityPercent: Number.isFinite(rarityPercent) ? rarityPercent : null,
    unlockedAtSec,
    ttl,
  };

  try {
    await ddb.send(new PutCommand({
      TableName: EVENTS_TABLE,
      Item: item,
      ConditionExpression: "attribute_not_exists(PK) AND attribute_not_exists(SK)",
    }));
  } catch (e) {
    const msg = e?.name || e?.message || String(e);
    if (String(msg).includes("ConditionalCheckFailed")) return;
    console.log(`[system] Failed to record event: ${msg}`);
  }
}

/* -------------------- DynamoDB guardrail + state -------------------- */

function approxUtf8Bytes(obj) {
  const s = JSON.stringify(obj);
  return Buffer.byteLength(s, "utf8");
}

function applyDdbSizeGuardrail(name, item) {
  const initialBytes = approxUtf8Bytes(item);
  if (initialBytes <= MAX_ITEM_BYTES) {
    return { item, truncated: false, bytes: initialBytes };
  }

  log(name, `⚠️ DynamoDB item nearing size limit: approxBytes=${initialBytes}. Truncating arrays (limit=${MAX_ITEM_BYTES}).`);

  const trimmed = {
    ...item,
    unlockedApiNames: undefined,
    lockedApiNames: undefined,
    unannouncedUnlockedApiNames: undefined,
    arraysTruncated: true,
    approxBytesBeforeTruncate: initialBytes,
  };

  for (const k of Object.keys(trimmed)) {
    if (trimmed[k] === undefined) delete trimmed[k];
  }

  const finalBytes = approxUtf8Bytes(trimmed);

  if (finalBytes > MAX_ITEM_BYTES) {
    log(name, `🚨 Still too large after truncation: approxBytes=${finalBytes}. Dropping announcedApiNames as last resort (may repost).`);
    delete trimmed.announcedApiNames;
    trimmed.announcedDropped = true;
    const finalFinalBytes = approxUtf8Bytes(trimmed);
    return { item: trimmed, truncated: true, bytes: finalFinalBytes };
  }

  return { item: trimmed, truncated: true, bytes: finalBytes };
}

async function getState(name, pk) {
  log(name, `DynamoDB GetItem PK=${pk}`);
  const out = await ddb.send(new GetCommand({ TableName: DDB_TABLE, Key: { PK: pk } }));

  const exists = !!out.Item;
  const announcedApiNames = out.Item?.announcedApiNames ?? out.Item?.announced ?? [];
  const platinumAnnounced = !!out.Item?.platinumAnnounced;

  log(name, `DynamoDB state loaded: exists=${exists} announcedCount=${announcedApiNames.length} platinumAnnounced=${platinumAnnounced}`);
  return { exists, announcedApiNames, platinumAnnounced };
}

async function putStateItem(name, item) {
  const { item: safeItem, truncated, bytes } = applyDdbSizeGuardrail(name, item);

  log(
    name,
    `DynamoDB PutItem PK=${safeItem.PK} gameTitle="${safeItem.gameTitle}" progress=${safeItem.progressText} announcedCount=${safeItem.announcedApiNames?.length ?? 0} platinumAnnounced=${!!safeItem.platinumAnnounced} approxBytes=${bytes}${truncated ? " (TRUNCATED)" : ""}`
  );

  await ddb.send(new PutCommand({ TableName: DDB_TABLE, Item: safeItem }));
}

/* -------------------- Discord posting -------------------- */

async function postDiscordPayload(name, webhookUrl, payload) {
  const res = await fetch(webhookUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const body = await res.text();
    log(name, `Discord webhook failed: ${res.status} ${body}`);
    throw new Error(`Discord webhook error ${res.status}: ${body}`);
  }
}

async function postDiscordEmbed(name, webhookUrl, embed) {
  log(name, `Posting Discord embed: achievement="${embed?.fields?.find(f => f.name === "Achievement")?.value ?? "unknown"}"`);
  await postDiscordPayload(name, webhookUrl, {
    username: `${name}'s Platinum Bot`,
    embeds: [embed],
  });
  log(name, "Discord post succeeded");
}

async function postPlatinumCongrats(name, webhookUrl, gameTitle) {
  const msg = `@everyone Congratulations on your shiny new ${gameTitle} platinum, ${name}! 🏆✨`;
  await postDiscordPayload(name, webhookUrl, {
    username: `${name}'s Platinum Bot`,
    content: msg,
    embeds: PLATINUM_IMAGE_URL ? [{ thumbnail: { url: PLATINUM_IMAGE_URL } }] : [],
  });
  log(name, "Platinum celebration post succeeded");
}

/* -------------------- Steam: game selection -------------------- */

async function resolveTargetGame({ name, steamId }) {
  log(name, "Resolving current or last played game from Steam");

  const summariesUrl =
    `https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/` +
    `?key=${encodeURIComponent(STEAM_API_KEY)}&steamids=${encodeURIComponent(steamId)}`;

  const summariesJson = await steamGet(name, summariesUrl);
  const player = summariesJson?.response?.players?.[0];

  if (player?.gameid) {
    const gameTitle = player.gameextrainfo ?? `Steam App ${player.gameid}`;
    log(name, `Is online and playing: ${gameTitle} (appid=${player.gameid})`);
    return { appid: String(player.gameid), gameTitle, source: "currently_playing" };
  }

  log(name, "Not currently in-game. Checking recently played games.");

  const recentUrl =
    `https://api.steampowered.com/IPlayerService/GetRecentlyPlayedGames/v0001/` +
    `?key=${encodeURIComponent(STEAM_API_KEY)}&steamid=${encodeURIComponent(steamId)}&count=1`;

  const recentJson = await steamGet(name, recentUrl);
  const game = recentJson?.response?.games?.[0];

  if (game?.appid) {
    const gameTitle = game.name ?? `Steam App ${game.appid}`;
    log(name, `Is not online but their last played game was: ${gameTitle} (appid=${game.appid})`);
    return { appid: String(game.appid), gameTitle, source: "recently_played" };
  }

  log(name, "Is not online and has no recently played games.");
  return null;
}

/* -------------------- Rarity helpers -------------------- */

function rarityTier(pct) {
  if (pct < 1) return "Legendary";
  if (pct < 5) return "Epic";
  if (pct < 20) return "Rare";
  if (pct < 50) return "Uncommon";
  return "Common";
}

function formatRarityLine(pct) {
  return `🏆 Rarity: ${rarityTier(pct)} (${pct.toFixed(2)}% of players)`;
}

function rarityColor(pct) {
  if (pct < 1) return 0xF1C40F;
  if (pct < 5) return 0xE67E22;
  if (pct < 20) return 0x9B59B6;
  if (pct < 50) return 0x3498DB;
  return 0x2ECC71;
}

/* -------------------- Lazy schema/rarity caches -------------------- */

const schemaCache = new Map(); // appid -> { schemaByApi, totalCount, schemaGameName }
const rarityCache = new Map(); // appid -> Map(apiname -> percent) OR null

async function getSchemaForApp({ name, appid }) {
  if (schemaCache.has(appid)) return schemaCache.get(appid);

  const schemaUrl =
    `https://api.steampowered.com/ISteamUserStats/GetSchemaForGame/v0002/` +
    `?key=${encodeURIComponent(STEAM_API_KEY)}&appid=${encodeURIComponent(appid)}`;

  log(name, `Fetching game schema (appid=${appid})`);
  const schemaJson = await steamGet(name, schemaUrl);

  const schemaGameName = schemaJson?.game?.gameName ?? null;
  const schemaAch = schemaJson?.game?.availableGameStats?.achievements ?? [];
  const totalCount = schemaAch.length;
  const schemaByApi = new Map(schemaAch.map((a) => [a.name, a]));

  const cached = { schemaGameName, schemaByApi, totalCount };
  schemaCache.set(appid, cached);
  return cached;
}

async function getRarityForApp({ name, appid }) {
  if (rarityCache.has(appid)) return rarityCache.get(appid);

  const url =
    `https://api.steampowered.com/ISteamUserStats/GetGlobalAchievementPercentagesForApp/v0002/` +
    `?gameid=${encodeURIComponent(appid)}`;

  log(name, `Fetching global achievement percentages (rarity) (appid=${appid})`);
  const json = await steamGet(name, url);

  const arr = json?.achievementpercentages?.achievements ?? [];
  if (!Array.isArray(arr) || arr.length === 0) {
    log(name, `No rarity data returned for appid=${appid}`);
    rarityCache.set(appid, null);
    return null;
  }

  const map = new Map(arr.map((x) => [x.name, Number(x.percent)]));
  rarityCache.set(appid, map);
  return map;
}

/* -------------------- main per-user processing -------------------- */

async function processOneUser(user) {
  const name = user.name;
  const steamId = String(user.steamId).trim();
  const webhookUrl = user.webhookUrl || DISCORD_WEBHOOK_URL;
  const tz = user.timezone || TIMEZONE;
  const windowSeconds = Number(user.windowSeconds || DEFAULT_WINDOW) || DEFAULT_WINDOW;

  log(name, "User processing started");

  const target = await resolveTargetGame({ name, steamId });
  if (!target) {
    log(name, "No current or recently played game found. Exiting user.");
    return { ok: true, name, posted: 0, reason: "no_current_or_recent_game" };
  }

  const { appid, gameTitle: resolvedGameTitle, source } = target;
  log(name, `Selected game: ${resolvedGameTitle} (appid=${appid}, source=${source})`);

  const achievementsUrl = `https://steamcommunity.com/profiles/${steamId}/stats/${appid}/achievements/`;

  // 1) Fetch player achievements FIRST (cheap-ish, and lets us decide whether we need schema/rarity)
  const playerUrl =
    `https://api.steampowered.com/ISteamUserStats/GetPlayerAchievements/v0001/` +
    `?appid=${encodeURIComponent(appid)}&key=${encodeURIComponent(STEAM_API_KEY)}&steamid=${encodeURIComponent(steamId)}`;

  log(name, `Fetching player achievements (steamid=${steamId}, appid=${appid})`);
  const playerJson = await steamGet(name, playerUrl);
  const playerAch = playerJson?.playerstats?.achievements ?? [];

  const unlocked = playerAch
    .filter((a) => Number(a.achieved) === 1)
    .map((a) => ({ apiname: a.apiname, unlocktime: a.unlocktime || 0 }))
    .sort((a, b) => b.unlocktime - a.unlocktime);

  const lockedApiNames = playerAch.filter((a) => Number(a.achieved) === 0).map((a) => a.apiname);
  const unlockedApiNames = unlocked.map((a) => a.apiname);

  // 2) Load DDB state, compute "toPost" BEFORE schema/rarity calls
  const pk = `steam#${steamId}#app#${appid}`;
  const nowSec = Math.floor(Date.now() / 1000);
  const cutoff = nowSec - windowSeconds;

  const { exists, announcedApiNames, platinumAnnounced } = await getState(name, pk);
  let announcedSet = new Set(announcedApiNames);
  let platinumFlag = platinumAnnounced;

  const unlockedRecent = unlocked.filter((a) => a.unlocktime > 0 && a.unlocktime >= cutoff);
  log(name, `Recent window: last ${windowSeconds}s. recentUnlocked=${unlockedRecent.length}`);

  if (!exists) {
    log(name, "First time seeing this game. Bootstrapping 'seen' achievements.");
    announcedSet = new Set(unlockedApiNames);
  }

  const toPost = exists
    ? unlockedRecent.filter((a) => !announcedSet.has(a.apiname))
    : unlockedRecent;

  // 3) Lazy-fetch schema/rarity ONLY if posting
  let schemaByApi = null;
  let totalCount = null;
  let rarityMap = null;

  // Game title: prefer presence name; schema name only used if needed
  let gameTitle = resolvedGameTitle;

  if (toPost.length > 0) {
    const schema = await getSchemaForApp({ name, appid });
    schemaByApi = schema.schemaByApi;
    totalCount = schema.totalCount;

    // Only fetch rarity if posting (optional)
    rarityMap = await getRarityForApp({ name, appid });

    // If schema returns a better title and presence title is missing, use it.
    if (!gameTitle) gameTitle = schema.schemaGameName || gameTitle;
    if (schema.schemaGameName && schema.schemaGameName.startsWith("ValveTestApp")) {
      log(name, `Schema placeholder "${schema.schemaGameName}", keeping resolved title "${resolvedGameTitle}"`);
    }
  }

  // 4) Even if we didn't fetch schema, we still want progress counts.
  // If totalCount unknown, fall back to unlocked+locked from player list (it matches total achievements in player response)
  const unlockedCount = unlockedApiNames.length;
  const lockedCount = lockedApiNames.length;

  const totalFallback = unlockedCount + lockedCount;
  const totalAchievements = (typeof totalCount === "number" ? totalCount : totalFallback);

  const pctComplete = percent(unlockedCount, totalAchievements);
  const progressText = `${unlockedCount}/${totalAchievements} (${pctComplete}%)`;
  log(name, `Progress: ${progressText}`);

  const isPlatinum = totalAchievements > 0 && unlockedCount === totalAchievements;

  // 5) Post embeds if needed
  if (toPost.length === 0) {
    log(name, "No new recent achievements to post.");
  } else {
    log(name, `Found ${toPost.length} new recent achievement(s) to post`);

    for (const a of toPost) {
      const meta = schemaByApi?.get(a.apiname);
      const achievementName = meta?.displayName ?? a.apiname;
      const achievementDesc = meta?.description ?? "Hidden Achievement";
      const iconUrl = meta?.icon ?? null;

      let rarityPct = null;
      if (rarityMap && rarityMap.has(a.apiname)) {
        const v = rarityMap.get(a.apiname);
        if (Number.isFinite(v)) rarityPct = v;
      }

      const embedColor = rarityPct === null ? 0xE74C3C : rarityColor(rarityPct);

      const fields = [
        { name: "Achievement", value: achievementName, inline: false },
        { name: "Achievement Description:", value: achievementDesc, inline: false },
        { name: "Unlocked On:", value: formatLocalDateFromUnix(a.unlocktime, tz), inline: false },
        { name: `Total ${gameTitle} Progress:`, value: `${unlockedCount}/${totalAchievements} — ${pctComplete}%`, inline: false },
      ];

      if (rarityPct !== null) {
        fields.push({ name: "Rarity", value: formatRarityLine(rarityPct), inline: false });
      }

      const embed = {
        color: embedColor,
        title: `${name} unlocked a new achievement in ${gameTitle}, they are now ${pctComplete}% complete.`,
        url: achievementsUrl,
        ...(iconUrl ? { thumbnail: { url: iconUrl } } : {}),
        fields,
      };

      await postDiscordEmbed(name, webhookUrl, embed);

      await recordAchievementEvent({
        name,
        steamId,
        appid,
        gameTitle,
        achievementApiName: a.apiname,
        achievementDisplayName: achievementName,
        rarityPercent: rarityPct,
        unlockedAtSec: a.unlocktime,
      });

      announcedSet.add(a.apiname);
    }
  }

  // 6) Platinum celebration (only once, only if we posted something this run)
  if (toPost.length > 0 && isPlatinum && !platinumFlag) {
    await postPlatinumCongrats(name, webhookUrl, gameTitle);
    platinumFlag = true;
  }

  // 7) Persist rich DynamoDB record every run
  const item = {
    PK: pk,
    name,
    steamId,
    appid,
    gameTitle,

    totalAchievements,
    unlockedCount,
    lockedCount,
    progressText,

    unlockedApiNames,
    lockedApiNames,
    announcedApiNames: Array.from(announcedSet),
    unannouncedUnlockedApiNames: unlockedApiNames.filter((api) => !announcedSet.has(api)),

    platinumAnnounced: !!platinumFlag,
    updatedAt: nowSec,
  };

  await putStateItem(name, item);

  log(name, "User processing complete");
  return { ok: true, name, posted: toPost.length, appid, gameTitle, progressText, platinum: isPlatinum };
}

/* -------------------- concurrency runner -------------------- */

async function runWithConcurrency(items, worker, concurrency) {
  const results = new Array(items.length);
  let idx = 0;

  async function runner() {
    while (true) {
      const current = idx++;
      if (current >= items.length) return;
      try {
        results[current] = await worker(items[current]);
      } catch (e) {
        results[current] = { ok: false, error: e?.message ?? String(e) };
      }
    }
  }

  const runners = Array.from({ length: Math.min(concurrency, items.length) }, () => runner());
  await Promise.all(runners);
  return results;
}

/* -------------------- Lambda handler -------------------- */

export async function handler() {
  const users = parseUsers();
  console.log(
    `[system] Starting run for ${users.length} user(s). Concurrency=${MAX_CONCURRENCY}. DDB_ITEM_MAX_BYTES=${MAX_ITEM_BYTES}. EVENTS_TABLE=${EVENTS_TABLE || "(disabled)"}`
  );

  const results = await runWithConcurrency(users, processOneUser, MAX_CONCURRENCY);

  const postedTotal = results.reduce((sum, r) => sum + (r?.posted || 0), 0);
  const platinumTotal = results.reduce((sum, r) => sum + (r?.platinum ? 1 : 0), 0);

  console.log(`[system] Run complete. postedTotal=${postedTotal} platinumUsers=${platinumTotal}`);
  return { ok: true, users: users.length, postedTotal, results };
}
