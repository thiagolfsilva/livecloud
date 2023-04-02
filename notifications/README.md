# Notifications

The funciton queries the API on AWS for data related to current arbitrage opportunities, queries the current subscribers from Firebase, and sends notifications to the users.

Deployed with:

`gcloud functions deploy telegram_notifications --gen2 --region=asia-southeast1 --runtime=python310 --set-env-vars TELEGRAM_BOT_TOKEN=<telegram_token> --entry-point telegram_notifications --trigger-topic tgnotifications`
