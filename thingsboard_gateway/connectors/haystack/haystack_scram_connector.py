import logging
import requests
from thingsboard_gateway.connectors.connector import Connector
import scramp

logger = logging.getLogger("HaystackScramConnector")

class HaystackScramConnector(Connector):
    def __init__(self, gateway, config):
        super().__init__(gateway, config)
        self.config = config
        self.server_url = config.get("server_url")
        self.username = config.get("username")
        self.password = config.get("password")
        self.scram_mechanism = config.get("scram_mechanism", "SCRAM-SHA-256")
        self.authenticated = False
        self.auth_token = None
        self.scram_client = scramp.ScramClient(self.scram_mechanism)

    def start(self):
        logger.info("Starting Haystack SCRAM Connector...")
        self._authenticate()

    def stop(self):
        logger.info("Stopping Haystack SCRAM Connector...")

    def _authenticate(self):
        try:
            # --- Step 1: Generate client first message ---
            client_first = self.scram_client.get_client_first_message(self.username)
            logger.debug("Client first message: %s", client_first)

            # --- Step 2: Send client_first to server's /scram/start endpoint ---
            server_first = self.receive_server_first(client_first)
            logger.debug("Received server first message: %s", server_first)

            # --- Step 3: Process server-first message to generate client final message ---
            client_final = self.scram_client.process_server_first_message(server_first, self.password)
            logger.debug("Client final message: %s", client_final)

            # --- Step 4: Send client_final to server's /scram/finish endpoint ---
            final_data = self.receive_server_final(client_final)
            logger.debug("Received server final data: %s", final_data)

            # --- Step 5: Complete SCRAM handshake ---
            # The final_data should include a key "server_final" carrying the SCRAM message.
            self.scram_client.process_server_final_message(final_data.get("server_final", ""))
            self.authenticated = True

            # Extract auth token either from the response body or from headers (if available)
            self.auth_token = final_data.get("authToken")
            if not self.auth_token:
                logger.warning("No authToken received from SCRAM finish. Proceeding without token.")
            else:
                logger.info("Authentication token obtained: %s", self.auth_token)

            logger.info("SCRAM authentication successful.")

            # --- Step 6: Retrieve sample data after authentication ---
            self.get_data()
        except Exception as e:
            logger.error("Authentication error: %s", e)
            self.authenticated = False

    def receive_server_first(self, client_first):
        """Send the client first message and receive the server's challenge."""
        endpoint = f"{self.server_url}/scram/start"
        payload = {"client_first": client_first}
        try:
            response = requests.post(endpoint, json=payload)
            response.raise_for_status()
            data = response.json()
            server_first = data.get("server_first")
            if not server_first:
                raise ValueError("No server_first received in response")
            return server_first
        except Exception as e:
            logger.error("Error in receive_server_first: %s", e)
            raise

    def receive_server_final(self, client_final):
        """Send the client final message and receive the final server message along with auth token."""
        endpoint = f"{self.server_url}/scram/finish"
        payload = {"client_final": client_final}
        try:
            response = requests.post(endpoint, json=payload)
            response.raise_for_status()
            data = response.json()
            # Optionally, check headers for Authentication-Info
            if "Authentication-Info" in response.headers:
                header_val = response.headers["Authentication-Info"]
                # Example header: "authToken=XYZ123ABC, hash=SHA-256, data=..."
                parts = header_val.split("authToken=")
                if len(parts) > 1:
                    token = parts[1].split(",")[0].strip()
                    data["authToken"] = token
            if "server_final" not in data:
                raise ValueError("No server_final found in response")
            return data
        except Exception as e:
            logger.error("Error in receive_server_final: %s", e)
            raise

    def get_data(self):
        """Retrieve sample data from the server using the authenticated session."""
        endpoint = f"{self.server_url}/data"
        headers = {"Accept": "application/json"}
        if self.auth_token:
            headers["Authorization"] = f"BEARER {self.auth_token}"
        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            data = response.json()
            logger.info("Data received: %s", data)
        except Exception as e:
            logger.error("Error fetching data: %s", e)

    # --- Optional callbacks ---
    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass
