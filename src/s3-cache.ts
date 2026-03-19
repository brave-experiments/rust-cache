import * as core from "@actions/core";
import awsLite from "@aws-lite/client";
// @ts-ignore
import awsS3 from "@aws-lite/s3";
import { spawn } from "child_process";
import { Readable, Transform } from "stream";

let _client: any = null;

async function getClient() {
  if (!_client) {
    const region =
      core.getInput("s3-region") ||
      process.env.AWS_REGION ||
      process.env.AWS_DEFAULT_REGION;
    _client = await awsLite({ region, plugins: [awsS3] });
  }
  return _client;
}

export function getBucket(): string {
  return core.getInput("s3-bucket");
}

export function isFeatureAvailable(): boolean {
  return !!getBucket();
}

export async function restoreCache(
  paths: string[],
  primaryKey: string,
  restoreKeys?: string[],
  options?: { lookupOnly?: boolean },
): Promise<string | undefined> {
  void paths;
  const bucket = getBucket();
  const aws = await getClient();

  // Try exact primary key first, then each restore key (prefix match)
  for (const key of [primaryKey, ...(restoreKeys ?? [])]) {
    let matchedKey: string | undefined;

    try {
      await aws.S3.HeadObject({ Bucket: bucket, Key: key });
      matchedKey = key;
    } catch {
      // Not an exact match — try prefix listing
      const result = await aws.S3.ListObjectsV2({ Bucket: bucket, Prefix: key, MaxKeys: 1 });
      const obj = result.Contents?.[0];
      if (obj?.Key) {
        matchedKey = obj.Key;
      }
    }

    if (!matchedKey) continue;

    if (!options?.lookupOnly) {
      await downloadAndExtract(aws, bucket, matchedKey);
    }
    return matchedKey;
  }

  return undefined;
}

export async function saveCache(paths: string[], key: string): Promise<string> {
  const bucket = getBucket();
  const aws = await getClient();

  const tar = spawn("tar", ["-czf", "-", "-C", "/", "--", ...paths], {
    stdio: ["ignore", "pipe", "inherit"],
  });

  let uploaded = 0;
  const progress = new Transform({
    transform(chunk, _encoding, callback) {
      uploaded += chunk.length;
      callback(null, chunk);
    },
  });

  const interval = setInterval(() => {
    const mb = (uploaded / (1024 * 1024)).toFixed(1);
    core.info(`Uploading cache... ${mb} MB`);
  }, 1000);

  try {
    await aws.S3.Upload({ Bucket: bucket, Key: key, Body: tar.stdout.pipe(progress) });
  } finally {
    clearInterval(interval);
  }

  await new Promise<void>((resolve, reject) => {
    tar.on("close", (code) =>
      code === 0 ? resolve() : reject(new Error(`tar exited with code ${code}`)),
    );
    tar.on("error", reject);
  });

  return key;
}

async function downloadAndExtract(aws: any, bucket: string, key: string): Promise<void> {
  core.info(`Downloading from S3 bucket "${bucket}", key "${key}"`);

  const response = await aws.S3.GetObject({ Bucket: bucket, Key: key, streamResponsePayload: true });
  const stream = response.Body as Readable;
  core.info(`Content-Length: ${response.ContentLength} bytes`);

  await new Promise<void>((resolve, reject) => {
    const tar = spawn("tar", ["-xzf", "-", "-C", "/"], {
      stdio: ["pipe", "inherit", "inherit"],
    });

    let downloaded = 0;
    let lastLog = 0;
    let lastBytes = 0;
    const progress = new Transform({
      transform(chunk, _encoding, callback) {
        downloaded += chunk.length;
        const now = Date.now();
        if (now - lastLog >= 1000) {
          const elapsed = (now - lastLog) / 1000;
          const mb = (downloaded / (1024 * 1024)).toFixed(1);
          const mbps = ((downloaded - lastBytes) / (1024 * 1024) / elapsed).toFixed(1);
          core.info(`Downloading cache... ${mb} MB (${mbps} MB/s)`);
          lastLog = now;
          lastBytes = downloaded;
        }
        callback(null, chunk);
      },
    });

    stream.pipe(progress).pipe(tar.stdin);

    tar.on("close", (code) =>
      code === 0 ? resolve() : reject(new Error(`tar exited with code ${code}`)),
    );
    tar.on("error", reject);
    stream.on("error", reject);
  });
}
