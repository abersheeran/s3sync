import { AwsClient } from 'aws4fetch';

export default {
	async fetch(request, env, ctx): Promise<Response> {
		const { key } = await request.json() as { key: string };
		await env.Q.send({ key });
		return new Response('ok');
	},
	async queue(batch, env): Promise<void> {
		let messages = JSON.stringify(batch.messages);
		console.debug(`consumed from our queue: ${messages}`);
		const message = batch.messages[0];
		const { key } = message.body as { key: string };
		const file = await downloadLargeFile({ key, env });
		await uploadLargeFile({ key, file, env });
		message.ack();
	},
} satisfies ExportedHandler<Env>;

interface DownloadOptions {
	key: string;
	env: Env;
}

async function downloadLargeFile({
	key,
	env,
}: DownloadOptions): Promise<ReadableStream> {
	const aws = new AwsClient({
		service: 's3',
		accessKeyId: env.SRC_AWS_ACCESS_KEY_ID,
		secretAccessKey: env.SRC_AWS_SECRET_ACCESS_KEY,
		region: env.SRC_AWS_REGION,
	});

	const endpoint: string = `https://${env.SRC_S3_ENDPOINT}`;

	const response: Response = await aws.fetch(`${endpoint}/${env.SRC_BUCKET_NAME}/${key}`);

	if (!response.ok) {
		throw new Error(`下载失败: ${await response.text()}`);
	}
	console.debug(`${key} 下载成功`);
	return response.body!;
}

interface UploadOptions {
	key: string;
	file: ReadableStream;
	env: Env;
}

interface UploadPart {
	ETag: string;
	PartNumber: number;
}

async function uploadLargeFile({
	key,
	file,
	env,
}: UploadOptions): Promise<void> {
	const aws = new AwsClient({
		service: 's3',
		accessKeyId: env.AWS_ACCESS_KEY_ID,
		secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
		region: env.AWS_REGION,
	});

	const endpoint: string = `https://${env.S3_ENDPOINT}`;

	// 初始化分片上传
	const initResponse: Response = await aws.fetch(`${endpoint}/${env.BUCKET_NAME}/${key}?uploads`, {
		method: 'POST',
	});

	if (!initResponse.ok) {
		throw new Error(`初始化上传失败: ${await initResponse.text()}`);
	}

	const initResult: string = await initResponse.text();
	const uploadId: string | undefined = initResult.match(/<UploadId>(.*)<\/UploadId>/)?.[1];

	if (!uploadId) {
		throw new Error('无法获取 UploadId');
	}

	const parts: UploadPart[] = [];
	let partNumber = 1;

	const bufferSize = 20 * 1024 * 1024; // 20MB
	let buffer = new Uint8Array(bufferSize);
	let bufferOffset = 0;

	try {
		const reader = file.getReader();

		while (true) {
			const { done, value } = await reader.read();
			if (done) break;

			let valueOffset = 0;
			while (valueOffset < value.length) {
				const spaceLeft = bufferSize - bufferOffset;
				const bytesToCopy = Math.min(spaceLeft, value.length - valueOffset);

				buffer.set(value.subarray(valueOffset, valueOffset + bytesToCopy), bufferOffset);
				bufferOffset += bytesToCopy;
				valueOffset += bytesToCopy;

				if (bufferOffset === bufferSize) {
					await uploadPart(buffer, partNumber);
					partNumber++;
					bufferOffset = 0;
				}
			}
		}

		// 上传剩余的缓冲区内容
		if (bufferOffset > 0) {
			await uploadPart(buffer.subarray(0, bufferOffset), partNumber);
		}

		// 完成分片上传
		const completeXml: string = `
      <CompleteMultipartUpload>
        ${parts.map(part => `
          <Part>
            <PartNumber>${part.PartNumber}</PartNumber>
            <ETag>${part.ETag}</ETag>
          </Part>
        `).join('')}
      </CompleteMultipartUpload>
    `;

		const completeResponse: Response = await aws.fetch(`${endpoint}/${env.BUCKET_NAME}/${key}?uploadId=${uploadId}`, {
			method: 'POST',
			body: completeXml,
		});

		if (!completeResponse.ok) {
			throw new Error(`完成上传失败: ${await completeResponse.text()}`);
		}

		console.debug(`${key} 上传成功`);
	} catch (error) {
		// 发生错误时尝试中止上传
		await aws.fetch(`${endpoint}/${env.BUCKET_NAME}/${key}?uploadId=${uploadId}`, {
			method: 'DELETE',
		}).catch((e: Error) => console.error('中止上传失败:', e));

		throw error;
	}

	async function uploadPart(data: Uint8Array, partNumber: number) {
		const uploadUrl: string = `${endpoint}/${env.BUCKET_NAME}/${key}?partNumber=${partNumber}&uploadId=${uploadId}`;
		const uploadResponse: Response = await aws.fetch(uploadUrl, {
			method: 'PUT',
			body: data,
		});

		if (!uploadResponse.ok) {
			throw new Error(`分片 ${partNumber} 上传失败`);
		}

		const etag: string | null = uploadResponse.headers.get('ETag');
		if (!etag) {
			throw new Error(`分片 ${partNumber} 未返回 ETag`);
		}

		console.debug(`${key} 分片 ${partNumber} 上传成功: ${etag}`);
		parts.push({
			ETag: etag,
			PartNumber: partNumber,
		});
	}
}
