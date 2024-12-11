import { WorkflowEntrypoint, WorkflowStep, WorkflowEvent } from 'cloudflare:workers';

import { AwsClient } from 'aws4fetch';

async function getFileSize(key: string, env: Env): Promise<number> {
	const aws = new AwsClient({
		service: 's3',
		accessKeyId: env.SRC_AWS_ACCESS_KEY_ID,
		secretAccessKey: env.SRC_AWS_SECRET_ACCESS_KEY,
		region: env.SRC_AWS_REGION,
	});

	const endpoint: string = `https://${env.SRC_S3_ENDPOINT}`;
	const file_url = `${endpoint}/${env.SRC_BUCKET_NAME}/${key}`;

	// 发送 HEAD 请求获取文件元数据
	const response = await aws.fetch(file_url, {
		method: 'HEAD'
	});

	if (!response.ok) {
		throw new Error(`获取文件大小失败: ${response.statusText}`);
	}

	const contentLength = response.headers.get('Content-Length');
	if (!contentLength) {
		throw new Error('无法获取文件大小信息');
	}

	return parseInt(contentLength, 10);
}

async function getFileRange(
	key: string,
	start: number,
	end: number,
	env: Env
): Promise<ReadableStream> {
	const aws = new AwsClient({
		service: 's3',
		accessKeyId: env.SRC_AWS_ACCESS_KEY_ID,
		secretAccessKey: env.SRC_AWS_SECRET_ACCESS_KEY,
		region: env.SRC_AWS_REGION,
	});

	const endpoint: string = `https://${env.SRC_S3_ENDPOINT}`;
	const file_url = `${endpoint}/${env.SRC_BUCKET_NAME}/${key}`;

	// 发送带有 Range 头的请求获取指定范围的数据
	const response = await aws.fetch(file_url, {
		method: 'GET',
		headers: {
			'Range': `bytes=${start}-${end}`
		}
	});

	if (!response.ok && response.status !== 206) {
		throw new Error(`获取文件范围数据失败: ${response.statusText}`);
	}

	return response.body!;
}

export class SyncWorkflow extends WorkflowEntrypoint<Env, { key: string }> {
	async run(event: WorkflowEvent<{ key: string }>, step: WorkflowStep) {
		const { key } = event.payload;

		const file_size = await step.do('Get file size', {
			retries: {
				limit: 23,
				delay: '10 second',
				backoff: 'exponential',
			},
			timeout: '1 hour'
		}, async () => await getFileSize(key, this.env));

		const endpoint: string = `https://${this.env.S3_ENDPOINT}`;
		const file_url = `${endpoint}/${this.env.BUCKET_NAME}/${key}`;
		const aws = new AwsClient({
			service: 's3',
			accessKeyId: this.env.AWS_ACCESS_KEY_ID,
			secretAccessKey: this.env.AWS_SECRET_ACCESS_KEY,
			region: this.env.AWS_REGION,
		});

		const upload_id = await step.do('Init upload', {
			retries: {
				limit: 23,
				delay: '10 second',
				backoff: 'exponential',
			},
			timeout: '1 minute'
		}, async () => {
			const initResponse: Response = await aws.fetch(`${file_url}?uploads`, {
				method: 'POST',
			});
			if (!initResponse.ok) {
				throw new Error(`初始化上传失败: ${await initResponse.text()}`);
			}
			const initResult: string = await initResponse.text();
			const uploadId = initResult.match(/<UploadId>(.*)<\/UploadId>/)?.[1];
			return uploadId;
		});

		const uploadStep = (partNumber: number, start: number, end: number) => {
			return step.do(
				`Upload file parts ${partNumber}`,
				{
					retries: {
						limit: 23,
						delay: '30 second',
						backoff: 'exponential',
					},
					timeout: '1 hour'
				},
				async () => {
					const upload_url: string = `${file_url}?partNumber=${partNumber}&uploadId=${upload_id}`;
					const upload_response: Response = await aws.fetch(upload_url, {
						method: 'PUT',
						body: await getFileRange(key, start, end, this.env),
					});

					if (!upload_response.ok) {
						throw new Error(`分片 ${partNumber} 上传失败: ${upload_response.status} ${await upload_response.text()}`);
					}

					const etag: string | null = upload_response.headers.get('ETag');
					if (!etag) {
						throw new Error(`分片 ${partNumber} 未返回 ETag`);
					}

					return {
						ETag: etag,
						PartNumber: partNumber,
					};
				},
			);
		}

		const part_size = 100 * 1024 * 1024; // maxsize per part
		const parts: { ETag: string; PartNumber: number }[] = [];

		for (let i = 0; i < file_size; i += part_size) {
			const start = i;
			const end = Math.min(i + part_size - 1, file_size - 1);
			parts.push(await uploadStep(i / part_size + 1, start, end));
		}

		await step.do('Complete upload', {
			retries: {
				limit: 23,
				delay: '30 second',
				backoff: 'exponential',
			},
			timeout: '1 year'
		}, async () => {
			// 完成分片上传
			const complete_xml: string = `
				<CompleteMultipartUpload>
					${parts.filter(part => part !== null).map(part => `
						<Part>
							<PartNumber>${part.PartNumber}</PartNumber>
							<ETag>${part.ETag}</ETag>
						</Part>
					`).join('')}
					</CompleteMultipartUpload>
				`;

			const complete_response: Response = await aws.fetch(`${file_url}?uploadId=${upload_id}`, {
				method: 'POST',
				body: complete_xml,
			});

			if (!complete_response.ok) {
				throw new Error(`完成上传失败: ${complete_response.status} ${await complete_response.text()}`);
			}
		});
	}
}

export default {
	async fetch(request, env, ctx): Promise<Response> {
		switch (request.method) {
			case 'GET': {
				// Get the status of an existing instance, if provided
				let id = new URL(request.url).searchParams.get("instanceId");
				if (!id) {
					return Response.json({
						error: 'instanceId is required',
					}, { status: 400 });
				}
				let instance = await env.SYNC_WORKFLOW.get(id);
				return Response.json(await instance.status());
			}

			case 'POST': {
				const { key } = await request.json() as { key: string };
				let instance = await env.SYNC_WORKFLOW.create({ params: { key } });
				return Response.json({
					instanceId: instance.id,
				});
			}

			case 'DELETE': {
				let id = new URL(request.url).searchParams.get("instanceId");
				if (!id) {
					return Response.json({
						error: 'instanceId is required',
					}, { status: 400 });
				}
				let instance = await env.SYNC_WORKFLOW.get(id);
				await instance.terminate();
				return Response.json({
					message: `Instance ${id} terminated`,
				});
			}

			default: {
				return Response.json({
					error: 'Method not allowed',
				}, { status: 405 });
			}
		}
	},
} satisfies ExportedHandler<Env>;
