from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    project: str = 'swift-delight-448019-p9'
    bucket: str = 'swift-delight-448019-p9-bucket'
    color: str = 'yellow'

settings = Settings()