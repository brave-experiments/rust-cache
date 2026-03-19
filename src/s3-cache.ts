import * as core from "@actions/core";
import {
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  S3Client,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { spawn } from "child_process";
import { Readable, Transform } from "stream";

function getS3Client(): S3Client {
  const region =
    core.getInput("s3-region") ||
    process.env.AWS_REGION ||
    process.env.AWS_DEFAULT_REGION;
  return new S3Client({ region });
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
  const bucket = getBucket();
  const client = getS3Client();

  // Try exact primary key first, then each restore key (prefix match)
  const exactKeys = [primaryKey];
  const prefixKeys = restoreKeys ?? [];

  for (const key of exactKeys) {
    try {
      await client.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
      if (!options?.lookupOnly) {
        await downloadAndExtract(client, bucket, key, paths);
      }
      return key;
    } catch {
      // not found, continue
    }
  }

  for (const prefix of prefixKeys) {
    const result = await client.send(
      new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, MaxKeys: 1 }),
    );
    const obj = result.Contents?.[0];
    if (!obj?.Key) {
      continue;
    }
    if (!options?.lookupOnly) {
      await downloadAndExtract(client, bucket, obj.Key, paths);
    }
    return obj.Key;
  }

  return undefined;
}

export async function saveCache(paths: string[], key: string): Promise<string> {
  const bucket = getBucket();
  const client = getS3Client();

  const tar = spawn("tar", ["-czf", "-", "-C", "/", "--", ...paths], {
    stdio: ["ignore", "pipe", "inherit"],
  });

  const upload = new Upload({
    client,
    params: {
      Bucket: bucket,
      Key: key,
      Body: tar.stdout as Readable,
    },
  });

  upload.on("httpUploadProgress", (progress) => {
    const mb = ((progress.loaded ?? 0) / (1024 * 1024)).toFixed(1);
    core.info(`Uploading cache... ${mb} MB uploaded (part ${progress.part})`);
  });

  await upload.done();

  await new Promise<void>((resolve, reject) => {
    tar.on("close", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`tar exited with code ${code}`));
      }
    });
    tar.on("error", reject);
  });

  return key;
}

async function downloadAndExtract(
  client: S3Client,
  bucket: string,
  key: string,
  _paths: string[],
): Promise<void> {
  core.info(`Downloading from S3 bucket "${bucket}", key "${key}"`);
  const response = await client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  core.info(`Content-Length: ${response.ContentLength} bytes`);

  if (!response.Body) {
    throw new Error(`Empty response body for key: ${key}`);
  }

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

    (response.Body as Readable).pipe(progress).pipe(tar.stdin);

    tar.on("close", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`tar exited with code ${code}`));
      }
    });
    tar.on("error", reject);
    (response.Body as Readable).on("error", reject);
  });
}
