from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str
    app_env: str

    stackspot_agent_url: str
    stackspot_auth_token: str
    stackspot_timeout_seconds: int

    openai_api_key: str | None = None
    openai_model: str = "gpt-4o-mini"

    class Config:
        env_file = ".env"


settings = Settings()