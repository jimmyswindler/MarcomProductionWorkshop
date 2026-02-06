import requests
import time
import os
import logging

class UPSAddressValidator:
    def __init__(self, client_id, client_secret, base_url="https://onlinetools.ups.com"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = base_url
        self.token = None
        self.token_expiry = 0
        self.logger = logging.getLogger("UPSValidator")

    def _get_token(self):
        """Retrieves or refreshes the OAuth2 token."""
        if self.token and time.time() < self.token_expiry:
            return self.token

        url = f"{self.base_url}/security/v1/oauth/token"
        payload = {'grant_type': 'client_credentials'}
        # Basic Auth for token endpoint uses Client ID and Secret
        auth = (self.client_id, self.client_secret)

        try:
            response = requests.post(url, data=payload, auth=auth)
            response.raise_for_status()
            data = response.json()
            
            self.token = data.get('access_token')
            expires_in = int(data.get('expires_in', 3600))
            # Set expiry with a small buffer (e.g., 60 seconds)
            self.token_expiry = time.time() + expires_in - 60
            
            return self.token
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get UPS token: {e}")
            if hasattr(e, 'response') and e.response:
                self.logger.error(f"UPS Response: {e.response.text}")
            return None

    def validate_address(self, address_lines, city, state, zip_code, country="US"):
        """
        Validates an address.
        Returns a dict:
        {
            'status': 'VALID', 'CORRECTED', 'AMBIGUOUS', 'INVALID', 'ERROR'
            'data': <Dict with keys: address1, address2, address3, city, state, zip, country> (if valid/corrected)
            'candidates': <List of candidate dicts> (if ambiguous)
            'raw_response': <Full JSON response>
        }
        """
        token = self._get_token()
        if not token:
            return {'status': 'ERROR', 'msg': 'Could not obtain API token'}

        url = f"{self.base_url}/api/addressvalidation/v2/1" # v2/1 = Address Validation
        
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
            'x-locale': 'en_US'
        }
        
        # Construct Request Body based on XAVRequestWrapper schema
        # AddressLine can be a list of up to 3 strings.
        
        # Clean inputs
        clean_lines = [l for l in address_lines if l and str(l).strip()]
        
        payload = {
            "XAVRequest": {
                "AddressKeyFormat": {
                    "AddressLine": clean_lines,
                    "PoliticalDivision2": city, # City
                    "PoliticalDivision1": state, # State
                    "PostcodePrimaryLow": str(zip_code).split('-')[0] if zip_code else "",
                    "CountryCode": country
                }
            }
        }

        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            xav_resp = data.get('XAVResponse', {})
            
            # --- Parse Indicators ---
            is_valid = xav_resp.get('ValidAddressIndicator') is not None
            is_ambiguous = xav_resp.get('AmbiguousAddressIndicator') is not None
            is_no_match = xav_resp.get('NoCandidatesIndicator') is not None
            
            candidates = xav_resp.get('Candidate', [])
            if isinstance(candidates, dict): candidates = [candidates] # Normalize to list
            
            # --- Logic ---
            
            # Case 1: Valid (UPS recognizes it exactly or close enough to be confident)
            if is_valid:
                # Even if valid, UPS might return a standardized version in the candidate list (usually 1 candidate)
                # We should use that standardized version.
                if candidates:
                    std_addr = self._parse_candidate_address(candidates[0])
                    return {'status': 'VALID', 'data': std_addr, 'raw_response': data}
                else:
                    # Should be rare for Valid indicator with no candidate, but implies input was perfect?
                    # Or we just return input as confirmed.
                    # Actually XAV usually returns the standardized form in Candidate even if valid.
                    return {'status': 'VALID', 'data': None, 'raw_response': data, 'msg': 'Valid but no candidate returned'}

            # Case 2: Ambiguous (Multiple Candidates usually, or just one that is a "guess")
            # User Rule: Auto-accept if len(candidates) == 1. Flag if > 1.
            if is_ambiguous:
                if len(candidates) == 1:
                    std_addr = self._parse_candidate_address(candidates[0])
                    return {'status': 'CORRECTED', 'data': std_addr, 'raw_response': data}
                elif len(candidates) > 1:
                    return {'status': 'AMBIGUOUS', 'candidates': [self._parse_candidate_address(c) for c in candidates], 'raw_response': data}
            
            # Case 3: No Candidates / Invalid
            if is_no_match:
                return {'status': 'INVALID', 'raw_response': data}

            # Fallback (Edge cases where UPS returns candidates but no indicators?)
            if candidates:
                if len(candidates) == 1:
                    std_addr = self._parse_candidate_address(candidates[0])
                    return {'status': 'CORRECTED', 'data': std_addr, 'raw_response': data}
                else:
                    return {'status': 'AMBIGUOUS', 'candidates': [self._parse_candidate_address(c) for c in candidates], 'raw_response': data}

            return {'status': 'INVALID', 'raw_response': data}

        except requests.exceptions.RequestException as e:
            self.logger.error(f"UPS API Request Failed: {e}")
            msg = str(e)
            if hasattr(e, 'response') and e.response:
                msg += f" | {e.response.text}"
            return {'status': 'ERROR', 'msg': msg}

    def _parse_candidate_address(self, candidate):
        """Extracts flattened address from a Candidate object."""
        key_fmt = candidate.get('AddressKeyFormat', {})
        
        # Lines
        lines = key_fmt.get('AddressLine', [])
        if isinstance(lines, str): lines = [lines]
        
        addr1 = lines[0] if len(lines) > 0 else ""
        addr2 = lines[1] if len(lines) > 1 else ""
        addr3 = lines[2] if len(lines) > 2 else ""
        
        return {
            'address1': addr1,
            'address2': addr2,
            'address3': addr3,
            'city': key_fmt.get('PoliticalDivision2', ''),
            'state': key_fmt.get('PoliticalDivision1', ''),
            'zip': key_fmt.get('PostcodePrimaryLow', ''),
            'zip_extension': key_fmt.get('PostcodeExtendedLow', ''),
            'country': key_fmt.get('CountryCode', ''),
            'is_residential': candidate.get('AddressClassification', {}).get('Code') == '2'
        }
