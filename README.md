## 概要

Pub/Sub 順序制御プロセッサ (Redis ロック利用)。
Google Cloud Pub/Sub の順序制御キー（Ordering Key）を利用しつつ、メッセージの「処理完了」までを保証する順序制御を実現するサンプルアプリケーション。
Cloud Pub/Sub の順序制御キーはメッセージの配信順序しか保証しないが、 Redis を組み合わせることで、同一キーのメッセージが厳密に 1 つずつ処理されることを保証。

- **同一キーのメッセージ**: 直前のメッセージの処理が完了するまで、次のメッセージの処理を開始しない（直列処理）。
- **異なるキーのメッセージ**: それぞれ独立して並列に処理される。

## 利用技術

- TypeScript
- Node.js
- Google Cloud Pub/Sub
- Redis
- Docker

## セットアップと実行手順

### 1\. プロジェクトのセットアップ

```bash
# 1. Subscriberを初期化
cd subscriber
npm install

# 2. Publisherを初期化
cd publisher
npm install
```

### 2\. 環境の起動

プロジェクトのルートディレクトリで以下のコマンドを実行し、Subscriber と Redis と Cloud Pub/Sub エミュレータをバックグラウンドで起動

```bash
docker-compose up --build -d subscriber
```

### 3\. 動作確認

Subscriber のログを監視して、メッセージを待ち受けている状態を確認

```bash
docker-compose logs -f subscriber
```

以下のようなログが表示されれば成功

```log
subscriber-1  | ✅ Pull Subscription projects/my-local-project/subscriptions/ordered-subscription created.
subscriber-1  |    - Message Ordering: Enabled
subscriber-1  | 🚀 Subscriber listening for messages on projects/my-local-project/subscriptions/ordered-subscription...
```

### 4\. メッセージの発行

別のターミナルを開き、以下のコマンドでメッセージを発行

```bash
docker-compose run --rm publisher
```

ログを監視しているターミナルで、メッセージが順序通りに処理されていく様子が確認できる

### 5\. 環境の停止

テストが完了したら、以下のコマンドで全てのコンテナを停止・削除

```bash
docker-compose down
```

## Pub/Sub サブスクリプションの設定

- **`ackDeadlineSeconds`**: 確認応答期限。
- **`retryPolicy`**: `nack` されたメッセージの再試行ポリシー。
  - `minimumBackoff`: 再試行までの最小待機時間。
  - `maximumBackoff`: 再試行までの最大待機時間。

## プロジェクト構造

```plaintext
ordered-processor-ts-sample/
├── docker-compose.yml       # Dockerサービス定義
├── publisher/               # メッセージ発行アプリ
│   ├── Dockerfile
│   ├── package.json
│   ├── tsconfig.json
│   └── src/index.ts         # CLI引数を受け取る発行スクリプト
└── subscriber/              # メッセージ受信・処理アプリ
    ├── Dockerfile
    ├── package.json
    ├── tsconfig.json
    └── src/index.ts         # Redisロックを使った順序制御ロジック
```

## 備考

- Subscriber の処理時間はランダムにしているため、同一キーのメッセージが順序通りに処理されていることが確認しやすい
- 本当は可変するキーに応じて並列処理が可能なキューイングシステムを実現したかったが、どう実現したらいいかわからず Pub/Sub の順序指定キー × Redis を使ったロック × nack を使った再試行でごまかした
  - この構成だと nack による再試行が多発する可能性がある
  - 長時間のロック競合でメッセージが保持期間を超過すると消失することもある
- 本構成は学習・検証目的であり、本番運用時は上記課題への対策が必要
