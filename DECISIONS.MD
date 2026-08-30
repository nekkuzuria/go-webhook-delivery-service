# Decisions

## 1. Delivery Semantics

- In this project, it guarantee at-least-once delivery. There is a retry mechanism to make sure the event will be delivered at least once.
- Why not at-most-once? Because if we choose this, we cannot do retry and there will be a risk of losing the event (which we don't want that).
- Why not exactly-once? It's difficult to guarantee exactly-once delivery since we use HTTP, where there can be a possibility that the event is already processed by the customer but the response is lost in the middle. This can cause our service to retry and deliver the same event again.
- Each event is delivered to all active subscriptions. Each subscription has its own delivery attempt with independent retry tracking.

## 2. Retry Behavior

- Default attempt is 10x (can be configured with `MAX_RETRIES` environment variable).
- Backoff: 30s → 1m → 5m → 15m → 30m → 1h → 5h → 10h → 24h. The delay increases after each failed attempt.
- After reaching the max retries, the delivery attempt will be permanently marked as `FAILED` in the database (terminal state), and it will not be retried again.

## 3. Long Outage

- The service will keep retrying according to the configured retry policy.
- Other subscriptions will not be affected by one subscription's outage.

## 4. Ordering

- Events are not ordered in this case. Because there is no requirement that indicates ordering is needed.
- Ordering should be used when the order of events matters and one event depends on the previous event being completed first.
- For example, in an order system, an `order.created` event should be processed before an `order.shipped` event. It would not make sense to process `order.shipped` before the order has been created.
- Another example, in a game, a `voucher.redeemed` event should be processed before a `reward.granted` event. Processing the reward first could result in a player receiving the reward without successfully redeeming the voucher.

## 5. Deliberately Left Out

- Subscription registration is currently hardcoded for simplicity.
- The service currently uses a single consumer. It is not designed for horizontal scaling yet.
- If the event volume increases significantly, keeping too much state in memory could become a bottleneck. 
