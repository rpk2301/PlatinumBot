import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand, PutCommand } from "@aws-sdk/lib-dynamodb";

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const {
  USERS, 
  STEAM_API_KEY,
  DISCORD_WEBHOOK_URL, 
  DDB_TABLE,
  TIMEZONE = "America/New_York",
  ACH_WINDOW_SECONDS = "900", //15min
  CONCURRENCY = "5", 
  PLATINUM_IMAGE_URL,
  DDB_ITEM_MAX_BYTES = "350000",
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
mustEnv("PLATINUM_IMAGE_URL")

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

/**
 * byte-size estimator for DynamoDB item guardrail.
 */
function approxUtf8Bytes(obj) {
  const s = JSON.stringify(obj);
  return Buffer.byteLength(s, "utf8");
}

/**
 * Guard rail: If item is getting too large, drop the biggest arrays and mark truncated.
 * We always keep announcedApiNames (needed for idempotency), plus counts + metadata.
 */
function applyDdbSizeGuardrail(name, item) {
  const initialBytes = approxUtf8Bytes(item);
  if (initialBytes <= MAX_ITEM_BYTES) {
    return { item, truncated: false, bytes: initialBytes };
  }

  log(
    name,
    `DynamoDB item nearing size limit: approxBytes=${initialBytes}. Truncating large arrays (limit=${MAX_ITEM_BYTES}).`
  );

  const trimmed = {
    ...item,
    // Drop biggest arrays first:
    unlockedApiNames: undefined,
    lockedApiNames: undefined,
    unannouncedUnlockedApiNames: undefined,
    arraysTruncated: true,
    approxBytesBeforeTruncate: initialBytes,
  };

  // Remove undefined keys (DynamoDBDocumentClient can ignore them, but i want this to be explicit)
  // I might change this later tbh
  for (const k of Object.keys(trimmed)) {
    if (trimmed[k] === undefined) delete trimmed[k];
  }

  const finalBytes = approxUtf8Bytes(trimmed);

  // If it's STILL too big, also drop announced list and keep only counts + flags.
  if (finalBytes > MAX_ITEM_BYTES) {
    log(
      name,
      `🚨 Still too large after truncation: approxBytes=${finalBytes}. Dropping announcedApiNames as last resort (may repost).`
    );

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

  log(
    name,
    `DynamoDB state loaded: exists=${exists} announcedCount=${announcedApiNames.length} platinumAnnounced=${platinumAnnounced}`
  );

  return { exists, announcedApiNames, platinumAnnounced, priorItem: out.Item || null };
}

async function putStateItem(name, item) {
  const { item: safeItem, truncated, bytes } = applyDdbSizeGuardrail(name, item);

  log(
    name,
    `DynamoDB PutItem PK=${safeItem.PK} gameTitle="${safeItem.gameTitle}" progress=${safeItem.progressText} announcedCount=${safeItem.announcedApiNames?.length ?? 0} platinumAnnounced=${!!safeItem.platinumAnnounced} approxBytes=${bytes}${truncated ? " (TRUNCATED)" : ""}`
  );

  await ddb.send(
    new PutCommand({
      TableName: DDB_TABLE,
      Item: safeItem,
    })
  );
}

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
  log(name, `Posting Discord embed: achievement="${embed?.fields?.[0]?.value ?? "unknown"}"`);

  const payload = {
    username: `${name}'s Platinum Bot`,
    embeds: [embed],
  };

  await postDiscordPayload(name, webhookUrl, payload);
  log(name, "Discord post succeeded");
}

async function postPlatinumCongrats(name, webhookUrl, gameTitle) {
  const msg = `@everyone Congratulations on your shiny new ${gameTitle} platinum, ${name}! 🏆✨`;

  const payload = {
    username: `${name}'s Platinum Bot`,
    content: msg,
    embeds: PLATINUM_IMAGE_URL ? [{ image: { url: PLATINUM_IMAGE_URL } }] : [],
  };

  log(name, `Posting platinum celebration message for "${gameTitle}"`);
  await postDiscordPayload(name, webhookUrl, payload);
  log(name, "Platinum celebration post succeeded");
}

/**
 * Determine which game to track:
 * 1) If currently playing → use presence
 * 2) Else → most recently played
 */
async function resolveTargetGame({ name, steamId }) {
  log(name, "Resolving current or last played game from Steam");

  // Currently playing
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

  // Recently played
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

// In-run cache for schema by appid
const schemaCache = new Map(); // appid -> { schemaGameName, schemaByApi, totalCount }

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
  log(name, `Checking Achievements for ${resolvedGameTitle} (appid=${appid}, source=${source})`);

  const { schemaGameName, schemaByApi, totalCount } = await getSchemaForApp({ name, appid });

  // Prefer real title (presence/recent); schema is fallback only
  const gameTitle = resolvedGameTitle || schemaGameName || `Steam App ${appid}`;
  if (schemaGameName && schemaGameName.startsWith("ValveTestApp")) {
    log(name, `Schema returned placeholder name "${schemaGameName}", using resolved name "${resolvedGameTitle}"`);
  }

  // Player achievements
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

  const unlockedCount = unlockedApiNames.length;
  const lockedCount = lockedApiNames.length;

  const pct = percent(unlockedCount, totalCount);
  const progressText = `${unlockedCount}/${totalCount} (${pct}%)`;

  log(name, `Progress: ${progressText}`);


  const pk = `steam#${steamId}#app#${appid}`;
  const nowSec = Math.floor(Date.now() / 1000);
  const cutoff = nowSec - windowSeconds;

  const { exists, announcedApiNames, platinumAnnounced } = await getState(name, pk);
  let announcedSet = new Set(announcedApiNames);
  let platinumFlag = platinumAnnounced;

  // Filter only recent unlocks
  const unlockedRecent = unlocked.filter((a) => a.unlocktime > 0 && a.unlocktime >= cutoff);
  log(name, `Recent window: last ${windowSeconds}s. recentUnlocked=${unlockedRecent.length}`);

  // First time seeing this game → prevent historical spam but still track state
  if (!exists) {
    log(name, "First time seeing this game. Bootstrapping 'seen' achievements.");
    announcedSet = new Set(unlockedApiNames); // mark all unlocked history as "seen"
  }

  // Achievements we will post now
  const toPost = exists
    ? unlockedRecent.filter((a) => !announcedSet.has(a.apiname))
    : unlockedRecent;

  // Platinum detection
  const isPlatinum = totalCount > 0 && unlockedCount === totalCount;

  // Send achievement embeds
  if (toPost.length === 0) {
    log(name, "No new recent achievements to post.");
  } else {
    log(name, `Found ${toPost.length} new recent achievement(s) to post`);

    for (const a of toPost) {
      const meta = schemaByApi.get(a.apiname);
      const achievementName = meta?.displayName ?? a.apiname;
      const achievementDesc = meta?.description ?? "Hidden Achievement";
      const iconUrl = meta?.icon ?? null;

      const embed = {
        color: 0xe74c3c,
        author: { name: `${name}'s Platinum Bot` },
        description: `**${name} unlocked a new achievement in ${gameTitle}, they are now ${pct}% complete.**`,
        ...(iconUrl ? { thumbnail: { url: iconUrl } } : {}),
        fields: [
          { name: `${name}'s Most Recent Achievement:`, value: achievementName, inline: false },
          { name: "Achievement Description:", value: achievementDesc, inline: false },
          { name: "Unlocked On:", value: formatLocalDateFromUnix(a.unlocktime, tz), inline: false },
          { name: `Total ${gameTitle} Progress:`, value: `${unlockedCount}/${totalCount} — ${pct}%`, inline: false },
        ],
        url: `https://steamcommunity.com/profiles/${steamId}/stats/${appid}/achievements/`,
      };

      await postDiscordEmbed(name, webhookUrl, embed);
      announcedSet.add(a.apiname);
    }
  }

  // Platinum celebration (only once) — only do it if we posted something this run
  if (toPost.length > 0 && isPlatinum && !platinumFlag) {
    await postPlatinumCongrats(name, webhookUrl, gameTitle);
    platinumFlag = true;
  }

  const item = {
    PK: pk,
    name,
    steamId,
    appid,
    gameTitle,

    totalAchievements: totalCount,
    unlockedCount,
    lockedCount,
    progressText,

    // These can be large; guardrail will drop them if needed:
    unlockedApiNames,
    lockedApiNames,
    announcedApiNames: Array.from(announcedSet),
    unannouncedUnlockedApiNames: unlockedApiNames.filter((api) => !announcedSet.has(api)),

    platinumAnnounced: !!platinumFlag,
    updatedAt: nowSec,
  };

  await putStateItem(name, item);

  log(name, "User processing complete");
  return {
    ok: true,
    name,
    posted: toPost.length,
    appid,
    gameTitle,
    progressText,
    platinum: isPlatinum,
  };
}

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

export async function handler() {
  const users = parseUsers();
  console.log(
    `[system] Starting run for ${users.length} user(s). Concurrency=${MAX_CONCURRENCY}. DDB_ITEM_MAX_BYTES=${MAX_ITEM_BYTES}`
  );

  const results = await runWithConcurrency(users, processOneUser, MAX_CONCURRENCY);

  const postedTotal = results.reduce((sum, r) => sum + (r?.posted || 0), 0);
  const platinumTotal = results.reduce((sum, r) => sum + (r?.platinum ? 1 : 0), 0);

  console.log(`[system] Run complete. postedTotal=${postedTotal} platinumUsers=${platinumTotal}`);
  return { ok: true, users: users.length, postedTotal, results };
}
