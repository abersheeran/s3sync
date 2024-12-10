import { WorkflowEntrypoint, WorkflowStep, WorkflowEvent } from 'cloudflare:workers';

import { AwsClient } from 'aws4fetch';


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
	return response.body!;
}

export class SyncWorkflow extends WorkflowEntrypoint<Env, { key: string }> {
	async run(event: WorkflowEvent<{ key: string }>, step: WorkflowStep) {
		const { key } = event.payload;
		const endpoint: string = `https://${this.env.S3_ENDPOINT}`;
		const file_url = `${endpoint}/${this.env.BUCKET_NAME}/${key}`;
		const aws = new AwsClient({
			service: 's3',
			accessKeyId: this.env.AWS_ACCESS_KEY_ID,
			secretAccessKey: this.env.AWS_SECRET_ACCESS_KEY,
			region: this.env.AWS_REGION,
		});

		const file = await downloadLargeFile({ key, env: this.env });

		const uploadId = await step.do('Init upload', {
			retries: {
				limit: 5,
				delay: '10 second',
				backoff: 'constant',
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

		const uploadStep = (value: Uint8Array, partNumber: number) => {
			return step.do(
				`Upload file parts ${partNumber}`,
				{
					retries: {
						limit: 5,
						delay: '30 second',
						backoff: 'constant',
					},
					timeout: '1 hour'
				},
				async () => {
					const uploadUrl: string = `${file_url}?partNumber=${partNumber}&uploadId=${uploadId}`;
					const uploadResponse: Response = await aws.fetch(uploadUrl, {
						method: 'PUT',
						body: value,
					});

					if (!uploadResponse.ok) {
						throw new Error(`分片 ${partNumber} 上传失败`);
					}

					const etag: string | null = uploadResponse.headers.get('ETag');
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

		const reader = file.getReader();
		const bufferSize = 20 * 1024 * 1024; // 20MB
		let buffer = new Uint8Array(bufferSize);
		let bufferOffset = 0;
		let partNumber = 1;
		const promises: Promise<{
			ETag: string;
			PartNumber: number;
		}>[] = [];

		try {
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
						promises.push(uploadStep(buffer, partNumber));
						partNumber++;
						bufferOffset = 0;
					}
				}
			}

			// 上传剩余的缓冲区内容
			if (bufferOffset > 0) {
				promises.push(uploadStep(buffer.subarray(0, bufferOffset), partNumber));
			}

			const parts = (await Promise.allSettled(promises)).map(result => {
				if (result.status === 'fulfilled') {
					return result.value;
				}
				throw new Error(`Upload part failed: ${result.reason}`);
			});

			await step.do('Complete upload', {
				retries: {
					limit: 5,
					delay: '10 second',
					backoff: 'constant',
				},
				timeout: '1 minute'
			}, async () => {
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

				const completeResponse: Response = await aws.fetch(`${file_url}?uploadId=${uploadId}`, {
					method: 'POST',
					body: completeXml,
				});

				if (!completeResponse.ok) {
					throw new Error(`完成上传失败: ${await completeResponse.text()}`);
				}
			});
		} catch (error) {
			// 发生错误时尝试中止上传
			await step.do('Abort upload', {
				retries: {
					limit: 5,
					delay: '10 second',
					backoff: 'constant',
				},
				timeout: '1 minute'
			}, async () => {
				await aws.fetch(`${file_url}?uploadId=${uploadId}`, {
					method: 'DELETE',
				})
			});
			throw error;
		}
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
