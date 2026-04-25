# Telegram → Rubika Transfer Bot

## **Overview**

This bot receives files and direct links from Telegram, prepares them in a controlled pipeline, and sends them to the connected Rubika account.
The flow is designed to be reliable under load, includes queue management, and supports recovery actions when upload fails.

---

## **Detailed Workflow**

1. **Authentication and account linking**

   * User receives a one-time `auth_key` from the Telegram bot.
   * The key is sent to the Rubika side to verify ownership.
   * After verification, the Telegram user is linked to a Rubika `guid`.

2. **Receiving input**

   * The bot accepts Telegram media messages (document, video, audio, image, etc.).
   * It also accepts direct `http/https` download links.
   * File metadata is normalized (safe file name, file size, origin type).

3. **Batch creation**

   * Messages sent close together are grouped into one processing batch.
   * A `batch_id` is created and tracked in the database.
   * Before processing starts, the user must confirm the batch explicitly.

4. **Validation and reservation**

   * User quota is checked.
   * Server disk reservation is created before heavy operations start.
   * If resources are insufficient, processing is rejected early.

5. **Download and preparation**

   * Files are downloaded from Telegram or the provided URL.
   * Progress is tracked and periodically updated.
   * Partial failures are handled with cancellation and cleanup logic.

6. **Compression and protection**

   * Downloaded files are packed with **فشرده سازی** and password protection.
   * The resulting package is prepared for Rubika delivery.
   * Password generation includes required special characters.

7. **Upload to Rubika**

   * The packaged output is uploaded to the user’s linked Rubika account.
   * Fallback upload methods are attempted when needed.
   * Timeout and retry windows are enforced to avoid stale jobs.

8. **Result and cleanup**

   * On success, user receives package details and password.
   * On failure, a retry option is offered for a limited period.
   * Temporary files are deleted and disk reservations are released.

---

## **Admin capabilities**

* Search users by Telegram chat id.
* View usage counters and remaining quotas.
* Increase/decrease user main quota.
* Review queue and server free disk state.
* Trigger cleanup operations for server-side stored files.

---

## **Technical notes**

* Max file size per item: `1 GB`
* SQLite is used for state management and lightweight persistence.
* Fully asynchronous architecture based on `asyncio`.
* Configuration values are loaded from `.env`.

## **Environment variables (important)**

* `RUBIKA_MIRROR_CHANNEL` (optional): mirror every successful uploaded file to this Rubika channel too.  
  Example: `data_saves` or `@data_saves` or a direct channel guid (`c0...`).
