from starlette.config import Config

# Config will be read from environment variables and/or ".env" files.
config = Config(env_prefix='GRAPAT_')

DEBUG = config('DEBUG', cast=bool, default=False)
USER = config('USER', cast=str, default='default')
