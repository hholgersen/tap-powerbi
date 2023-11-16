"""PowerBI Authentication."""


from singer import utils
from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta


class PowerBIAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for PowerBI."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the PowerBI API."""
        return {
            'grant_type': 'client_credentials',
            'scope': 'https://analysis.windows.net/powerbi/api/.default',
            'client_id': self.config["client_id"],
            'client_secret': self.config["client_secret"],
        }

    @classmethod
    def create_for_stream(cls, stream) -> "PowerBIAuthenticator":
        return cls(
            stream=stream,
            auth_endpoint=f'https://login.microsoftonline.com/{cls.config["tenant_id"]}/oauth2/v2.0/token',
        )

    def is_token_valid(self) -> bool:
        """Check if token is valid.

        Returns:
            True if the token is valid (fresh).
        """
        if self.last_refreshed is None:
            return False
        if not self.expires_in:
            return True
        if int(self.expires_in) > (utils.now() - self.last_refreshed).total_seconds():
            return True
        return False
