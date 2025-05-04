import os

from tinkoff.invest import Client
from tinkoff.invest.sandbox.client import SandboxClient
from dotenv import load_dotenv

load_dotenv()

TOKEN_SANDBOX = os.getenv("TOKEN_SANDBOX")
TOKEN_READONLY = os.getenv("TOKEN_READONLY")
IS_SANDBOX = os.getenv("IS_SANDBOX", "false").lower() == "true"

def get_sandbox_client() -> SandboxClient:
    return SandboxClient(TOKEN_SANDBOX)

def get_readonly_client() -> Client:
    return Client(TOKEN_READONLY)

