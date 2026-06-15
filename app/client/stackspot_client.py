from typing import Any

import httpx

from app.config.config import settings


class StackSpotClient:
    def __init__(self):
        self.agent_url = settings.stackspot_agent_url
        self.auth_token = settings.stackspot_auth_token
        self.timeout = settings.stackspot_timeout_seconds

    async def invoke_agent(self, payload: dict[str, Any]) -> dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Content-Type": "application/json",
        }

        request_body = {
            "input": payload
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                self.agent_url,
                headers=headers,
                json=request_body,
            )

            response.raise_for_status()
            return response.json()
