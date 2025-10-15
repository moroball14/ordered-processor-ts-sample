import { PubSub, Message } from '@google-cloud/pubsub';
import { createClient, RedisClientType } from 'redis';

// --- 環境変数から設定を読み込み ---
const GCLOUD_PROJECT = process.env.GCLOUD_PROJECT!;
const TOPIC_NAME = process.env.TOPIC_NAME!;
const SUBSCRIPTION_NAME = process.env.SUBSCRIPTION_NAME!;
const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = parseInt(process.env.REDIS_PORT || '6379', 10);
const LOCK_EXPIRATION_SECONDS = 300; // 5分

// --- Pub/SubとRedisのクライアントを初期化 ---
const pubsub = new PubSub({
  projectId: GCLOUD_PROJECT,
  ...(process.env.PUBSUB_EMULATOR_HOST && {
    apiEndpoint: process.env.PUBSUB_EMULATOR_HOST,
  }),
});

const redisClient: RedisClientType = createClient({
  url: `redis://${REDIS_HOST}:${REDIS_PORT}`,
});
redisClient.on('error', (err) => console.error('Redis Client Error', err));

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * 起動時にトピックとPullサブスクリプションが存在するか確認し、なければ作成する
 */
async function setupPubSub() {
  const topic = pubsub.topic(TOPIC_NAME);
  const [topicExists] = await topic.exists();
  if (!topicExists) {
    await topic.create();
    console.log(`✅ Topic ${topic.name} created.`);
  }

  const subscription = topic.subscription(SUBSCRIPTION_NAME);
  const [subscriptionExists] = await subscription.exists();
  if (!subscriptionExists) {
    await subscription.create({
      enableMessageOrdering: true,
      ackDeadlineSeconds: 20,
      retryPolicy: {
        // nack後の再試行までの最小待機時間 (例: 5秒)
        minimumBackoff: { seconds: 5 },
        // nack後の再試行までの最大待機時間 (例: 10秒)
        maximumBackoff: { seconds: 10 },
      },
    });
    console.log(`✅ Pull Subscription ${subscription.name} created.`);
    console.log(`   - Message Ordering: Enabled`);
  }
  return subscription;
}

/**
 * メッセージハンドラ: 受信したメッセージを一件ずつ処理する
 */
const messageHandler = async (message: Message) => {
  const { orderingKey } = message.attributes;
  const data = message.data.toString();

  if (!orderingKey) {
    console.warn(`Message without orderingKey received. Acknowledging.`);
    message.ack();
    return;
  }

  const lockKey = `lock:${orderingKey}`;
  let isLockAcquired = false;

  try {
    // 1. Redisでアトミックにロックを試行
    const result = await redisClient.set(lockKey, 'processing', {
      NX: true, // キーが存在しない場合のみセット
      EX: LOCK_EXPIRATION_SECONDS, // 有効期限を設定
    });
    isLockAcquired = result === 'OK';

    if (!isLockAcquired) {
      // 2. ロック失敗 -> Nack
      console.log(`[${orderingKey}] 🟡 Key is locked. Nacking message: "${data}". timestamp: "${new Date().toISOString()}"`);
      message.nack();
      return;
    }

    // 3. ロック成功 -> 本体処理
    console.log(`[${orderingKey}] ✅ Lock acquired. Processing message: "${data}". timestamp: "${new Date().toISOString()}"`);
    // --- ここに時間のかかる処理を実装 ---
    await sleep(2000 + Math.random() * 500);
    // ---------------------------------
    console.log(`[${orderingKey}] ✨ Processing finished for message: "${data}". timestamp: "${new Date().toISOString()}"`);

    // 4. 処理成功 -> Ack
    message.ack();
    console.log(`[${orderingKey}] 👍 Acked message: "${data}". timestamp: "${new Date().toISOString()}"`);

  } catch (error) {
    console.error(`[${orderingKey}] 🚨 Error processing message: "${data}"`, error);
    // 処理中にエラーが発生した場合も Nack
    message.nack();
  } finally {
    // 5. 最後に必ずロックを解放
    if (isLockAcquired) {
      await redisClient.del(lockKey);
      console.log(`[${orderingKey}] 🔑 Lock released.`);
    }
  }
};


/**
 * メインの処理ロジック
 */
async function main() {
  await redisClient.connect();
  const subscription = await setupPubSub();

  console.log(`🚀 Subscriber listening for messages on ${subscription.name}...`);

  subscription.on('message', messageHandler);

  subscription.on('error', error => {
    console.error('🚨 Received error:', error);
    process.exit(1);
  });
}

main().catch(console.error);