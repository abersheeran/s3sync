import { AwsClient } from 'aws4fetch';

export default {
	async fetch(request, env, ctx): Promise<Response> {
		const { key } = await request.json() as { key: string };
		await env.Q.send({ key });
		return new Response('ok');
	},
	async queue(batch, env): Promise<void> {
		let messages = JSON.stringify(batch.messages);
		console.log(`consumed from our queue: ${messages}`);
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
}: DownloadOptions): Promise<Blob> {
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
	return await response.blob();
}

interface UploadOptions {
	key: string;
	file: File | Blob;
	chunkSize?: number;
	onProgress?: (progress: number) => void;
	env: Env;
}

interface UploadPart {
	ETag: string;
	PartNumber: number;
}

async function uploadLargeFile({
	key,
	file,
	chunkSize = 10 * 1024 * 1024,
	onProgress,
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
	const totalChunks: number = Math.ceil(file.size / chunkSize);

	try {
		// 上传分片
		for (let partNumber = 1; partNumber <= totalChunks; partNumber++) {
			const start: number = (partNumber - 1) * chunkSize;
			const end: number = Math.min(start + chunkSize, file.size);
			const chunk: Blob = file.slice(start, end);

			const uploadUrl: string = `${endpoint}/${env.BUCKET_NAME}/${key}?partNumber=${partNumber}&uploadId=${uploadId}`;
			const uploadResponse: Response = await aws.fetch(uploadUrl, {
				method: 'PUT',
				body: chunk,
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

			if (onProgress) {
				onProgress((partNumber / totalChunks) * 100);
			}
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

	} catch (error) {
		// 发生错误时尝试中止上传
		await aws.fetch(`${endpoint}/${env.BUCKET_NAME}/${key}?uploadId=${uploadId}`, {
			method: 'DELETE',
		}).catch((e: Error) => console.error('中止上传失败:', e));

		throw error;
	}
}
