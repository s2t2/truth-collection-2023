# Deploying to Render

Login to Render dashboard.

Create a new background worker. Provide the URL of this repo.

Oh, each process costs $7. Why don't we use Heroku with many processes for  $7.

# Deploying to Heroku

Create a new server. Configure remote address:

```sh
git remote add heroku SERVER_ADDRESS
```

Set environment variables. Add [buildpack](https://github.com/gerynugrh/heroku-google-application-credentials-buildpack).

```sh
heroku config:set APP_ENV="production"
heroku config:set DATASET_ADDRESS="________._______production"

heroku config:set TRUTH_USERNAME="____________"
heroku config:set TRUTH_PASSWORD="____________"

# references local creds file (for buildpack):
heroku config:set GOOGLE_CREDENTIALS="$(< google-credentials.json)"
# references server creds (for package):
heroku config:set GOOGLE_APPLICATION_CREDENTIALS="google-credentials.json"
```

Deploy:

```sh
git push heroku main
```