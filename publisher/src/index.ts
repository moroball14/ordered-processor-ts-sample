import { PubSub } from '@google-cloud/pubsub';

// --- 環境変数から設定を読み込み ---
const GCLOUD_PROJECT = process.env.GCLOUD_PROJECT!;
const TOPIC_NAME = process.env.TOPIC_NAME!;

// --- Pub/Subクライアントの初期化 ---
const pubsub = new PubSub({
  projectId: GCLOUD_PROJECT,
  // エミュレータが設定されていればそれを使う
  ...(process.env.PUBSUB_EMULATOR_HOST && {
    apiEndpoint: process.env.PUBSUB_EMULATOR_HOST,
  }),
});


async function publishMessage(key: string, message: string) {
  const dataBuffer = Buffer.from(message);
  try {
    const messageId = await pubsub
      .topic(TOPIC_NAME)
      .publishMessage({ data: dataBuffer, attributes: { orderingKey: key } });
    console.log(`📤 Message ${messageId} published. (Key: ${key}, Message: "${message}")`);
  } catch (error) {
    console.error(`🚨 Received error while publishing key ${key}:`, error);
  }
}

async function main() {
  console.log('--- Publishing messages ---');

  // キーAのメッセージを送信
  await publishMessage('A', 'message-A1');
  await publishMessage('A', 'message-A2');
  await publishMessage('A', 'message-A3');
  
  // キーBのメッセージを送信
  await publishMessage('B', 'message-B1');
  await publishMessage('B', 'message-B2');

  console.log('--- All messages sent ---');
}

main().catch(console.error);