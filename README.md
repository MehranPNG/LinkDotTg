# Telegram → Rubika Transfer Bot

## Overview

This bot receives files or direct download links from Telegram, processes them, and uploads them to a connected Rubika account.

---

## Workflow

1. **Authentication**

   * User gets an `auth_key` from Telegram bot
   * Sends it to the bot in Rubika
   * Telegram account is linked to a Rubika `guid`

2. **Input**

   * Accepts Telegram media or HTTP/HTTPS links
   * Extracts file name and size

3. **Batching**

   * Files are grouped with a short delay
   * A `batch_id` is created and stored
   * User must confirm before processing

4. **Processing**

   * Disk space is reserved (~2× total size)
   * Files are downloaded (Telegram or URL)
   * All files are compressed into a password-protected ZIP

5. **Upload**

   * ZIP file is uploaded to Rubika
   * Fallback methods are used if needed

6. **Result**

   * On success: user receives file name and password
   * On failure: retry option is available (limited time)

7. **Cleanup**

   * Temporary files are removed
   * Disk space is released

---

## Notes

* Max file size per item: 1GB
* Uses SQLite for state management
* Fully async (`asyncio`)
* Config loaded from `.env`
