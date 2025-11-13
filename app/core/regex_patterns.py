from app.schemas import PatternRecognizer


class Regexes:
    IN_PAN = r'(?i)[A-Z]{3}[ABCFGHLJPTF]{1}[A-Z]{1}[0-9]{4}[A-Z]{1}'
    IN_AADHAR = r'[0-9]{4}[ -]?[0-9]{4}[ -]?[0-9]{4}'

    CREDIT_CARD = r'\b((4\d{3})|(5[0-5]\d{2})|(6\d{3})|(1\d{3})|(3\d{3}))[- ]?(\d{3,4})[- ]?(\d{3,4})[- ]?(\d{3,5})\b'

    EMAIL_ADDRESS = (
        r"(?i)\b((([!#$%&*+\-/=?^_`{|}~\w][!#$%&'*+\-/=?^_`{|}~\.\w]{0,}[!#$%&'*+\-/=?^_`"
        r"{|}~\w]))[@]\w+([-.]\w+)*\.\w+([-.]\w+)*)\b"
    )

    IBAN_CODE = r"(?i)\b([A-Z]{2}[ \-]?[0-9]{2})((?:[ \-]?[A-Z0-9]{3,5}){2,6})([ \-]?[A-Z0-9]{1,3})?\b"

    CRYPTO = r"(?i)\b[13][a-km-zA-HJ-NP-Z1-9]{26,33}\b"

    US_SSN = r"\b([0-9]{3})[-.]?([0-9]{2})[-.]?([0-9]{4})\b"

    UK_NHS = r'\b([0-9]{3})[- ]?([0-9]{3})[- ]?([0-9]{4})\b'

    US_ITIN = r'\b9\d{2}[- ]?(5\d|6[0-5]|7\d|8[0-8]|9([0-2]|[4-9]))[- ]?\d{4}\b'

    US_PASSPORT = r"(\b[0-9]{9}\b) | (?i)(\b[A-Z][0-9]{8}\b)"

    IP_ADDRESS = (
        r"(\b(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(?:25[0-5]|2[0-4][0-9]"
        r"|[01]?[0-9][0-9]?)\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b)|"
        r"(\b(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:"
        r"[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}"
        r"(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}"
        r"(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:"
        r"(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]"
        r")\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9])"
        r"{0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))\b)"
    )

    US_DRIVER_LICENSE = (
        r'(\b([A-Z][A-Z0-9*]{11})\b)|'
        r'(\b([A-Z][0-9]{3,6}|[A-Z][0-9]{5,9}|[A-Z][0-9]{6,8}|[A-Z][0-9]{4,8}|[A-Z][0-9]{9,11}|[A-Z]{1,2}[0-9]{5,6}|'
        r'H[0-9]{8}|V[0-9]{6}|X[0-9]{8}|A-Z]{2}[0-9]{2,5}|[A-Z]{2}[0-9]{3,7}|[0-9]{2}[A-Z]{3}[0-9]{5,6}|[A-Z][0-9]'
        r'{13,14}|[A-Z][0-9]{18}|[A-Z][0-9]{6}R|[A-Z][0-9]{9}|[A-Z][0-9]{1,12}|[0-9]{9}[A-Z]|[A-Z]{2}[0-9]{6}[A-Z]|'
        r'[0-9]{8}[A-Z]{2}|[0-9]{3}[A-Z]{2}[0-9]{4}|[A-Z][0-9][A-Z][0-9][A-Z]|[0-9]{7,8}[A-Z])\b)|'
        r'(\b([0-9]{6,14}|[0-9]{16})\b)'
    )

    MEDICAL_LICENSE = (
        r"(?i)[abcdefghjklmprstuxABCDEFGHJKLMPRSTUX]{1}[a-zA-Z]{1}\d{7}|[abcdefghjklmprstuxABCDEFGHJKLMPRSTUX]{1}9\d{7}"
    )

    US_BANK_NUMBER = r'\b[0-9]{8,17}\b'

    AWS_CREDENTIALS = (
        r'(?i)((\s*(aws|aws(_?)secret(_?)access(_?)key(?:(_?)id)?|sha)\s*=\s*)([0-9a-zA-Z/+]{40})(\s*|$))|'
        r'((\s*(aws|aws(_?)access(?:(_?)key|(_?)key(_?)id))\s*=\s*)(AKIA[0-9A-Z]{16})(\s*|$))|'
        r'(\s*(aws(_?)security(_?)token|aws(_?)session(_?)token)\s*=\s*)([A-Za-z0-9+/]{342}\.[A-Za-z0-9+/]{4}\.)'
        r'([A-Za-z0-9+/]{30})(\s*|$)'
    )

    AZURE_CREDENTIALS = (
        r'(?i)((\s*(azure(_?)storage(_?)account(_?)key)\s*=\s*)([A-Za-z0-9+/]{86}==|[A-Za-z0-9+/]{87}=|'
        r'[A-Za-z0-9+/]{88})(\s*|$))|'
        r'((\s*(azure(_?)ad(_?)client(_?)secret)\s*=\s*)([a-zA-Z0-9~!@#$%^&*()-=_+{}\[\];:'
        r'\'",.<>?]{32,})(\s*|$))|'
        r'((\s*(azure(_?)client(_?)id)\s*=\s*)([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(\s*|$))|'
        r'((\s*(azure(_?)secret(?:(_?)key)?)\s*=\s*)([a-zA-Z0-9~!@#$%^&*()-=_+{}\[\];:\'",.<>?]{32,35})(\s*|$))|'
        r'((\s*(azure(_?)access(?:(_?)key)?)\s*=\s*)\S{3,})|'
        r'((\s*(azure(_?)ad(_?)client(_?)secret)\s*=\s*)([a-zA-Z0-9~!@#$%^&*()-=_+{}\[\];:\'",.<>?]{32,})(\s*|$))'
    )

    GITHUB_CREDENTIALS = (
        r'(?i)(\s*(github(_?)token|github(_?)access(_?)token|github(_?)token|'
        r'github(_?)personal(_?)access(_?)token|github(_?)sha)\s*=\s*)([0-9a-zA-Z/+]{40})(\s*|$)'
    )

    STRIPE_CREDENTIALS = (
        r'(?i)((\s*stripe(_?)secret\s*=\s*)([a-zA-Z0-9]{24}\.[a-zA-Z0-9]{32})(\s*|$))|'
        r'((\s*stripe(_?)public(_?)key\s*=\s*)(pk_test_[a-zA-Z0-9]{24})(\s*|$))'
    )

    SSH_KEYS = (
        r'(?i)(\s*(ssh(-?)rsa|ssh(-?)dsa|ssh(-?)ecdsa|ssh(-?)ed25519|ecdsa(-?)sha2(-?)nistp[0-9]{3})\s*=?\s*)'
        r'((?:AAAA[0-9A-Za-z+/]+[=]{0,3})(?: [^@\s]+@[^@\s]+)?)(\s*|$)'
    )

    TWILIO_CREDENTIALS = r'(?i)\s*(twilio_?account_?sid|twilio_?auth_?token)\s*=\s*([a-zA-Z0-9]{32})\s*'

    CELERY_CREDENTIALS = (
        r'(?i)(\s*(celery(_?)broker(_?)url)\s*=\s*)(amqp[s]?://[a-zA-Z0-9_]+:[a-zA-Z0-9_]+@[a-zA-Z0-9_.]+:'
        r'[0-9]+/[a-zA-Z0-9_]+)(\s*|$)'
    )

    SENDGRID_CREDENTIALS = r'(?i)(\s*(send(_?)grid(_?)key|send(_?)grid(_?)pass(?:word))\s*=\s*)(SG\.[a-zA-Z0-9_]{22}\.[a-zA-Z0-9_]{43})(\s*|$)'

    GCP_CREDENTIALS = (
        r'(?i)(\s*((google|gcp).{0,20}?)\s*=\s*)(AIza[a-zA-Z0-9]{35})(\s*|$)|'
        r'(\s*((google|gcp).{3}?(oauth|auth).{3}?(token|password))\s*=\s*)([a-zA-Z0-9-_.]{40,255})(\s*|$)|'
        r'(\s*((google|gcp).{0,20}?)\s*=\s*)\S{3,}(\s*|$)'
    )

    AUTH0_CREDENTIALS = (
        r'(?i)(\s*(auth0.{0,20}?)\s*=\s*)([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(\s*|$)'
    )

    SNOWFLAKE_CREDENTIALS = r'(?i)(\s*(snowflake.{0,20}?)\s*=\s*)\S{3,}(\s*|$)'
    PRIVATE_CREDENTIALS = (
        r'(?i)(\s*(cognitive.{0,20}?)\s*=\s*)([a-zA-Z0-9]{32})(\s*|$)|'
        r'(\s*(service_?bus_?sas_?key)\s*=\s*)([a-zA-Z0-9~!@#$%^&*()-=_+{}\[\];:'
        r'\'",.<>?]{32,})(\s*|$)|'
        r'(\s*(project.{0,8}id)\s*=\s*)([a-z][-a-z0-9]{0,28}[a-z0-9])(\s*|$)|'
        r'(\s*(private.{0,20}?)\s*=\s*)([a-zA-Z0-9_-]+)(\s*|$)|'
        r'(\s*((client|user|account|login).{0,20}?)\s*=\s*)([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})(\s*|$)|'
        r'(\s*((client|user|account|login).{0,20}?)\s*=\s*)(4[0-9]{20})(\s*|$)|'
        r'(\s*(sha.{0,20}?)\s*=\s*)([0-9a-zA-Z/+]{0,})(\s*|$)|'
        r'(\s*(auth.{0,20}?)\s*=\s*)(https://accounts.google.com/o/oauth2/auth.*)(\s*|$)|'
        r'(\s*(token.{0,20}?)\s*=\s*)(https://oauth2.googleapis.com/token.*)(\s*|$)|'
        r'(\s*(client_?x509_?cert_?url|auth_?provider_?x509_?cert_?url)\s*=\s*)(https://www\.googleapis\.com/.+)(\s*|$)|'
        r'(\s*(tenant.{0,20}?)\s*=\s*)([a-zA-Z0-9]{3,})(\s*|$)|'
        r'(\s*(service_?bus_?sas_?key)\s*=\s*)([a-zA-Z0-9~!@#$%^&*()-=_+{}\[\];:\'"\.<>?]{44})(\s*|$)|'
        r'(\s+(secret_?token|api_?id|api_?key|secret(?:_key)?|auth_?token|pwd|'
        r'username|secretkey|token|database_?pass(?:word)?|db_?pass(?:word).{0,20}?)\s*=\s*)\S{3,}(\s*|$)'
    )

    OPEN_AI_KEY = r'(?i)(\s*(open_ai|open_?ai_?key|open_?ai_?api_?key)\s*=?\s*)([a-zA-Z0-9]{32})(\s*|$)'

    SECRET_EXCLUDE = r'(?i)(\(.*\))|(=\s*get)'  # todo: include to credential regexes

    INSURANCE_INFORMATION = (
        r'(?i)(\s*(blue(?:_?shield)?(?:_?member)?(?:_?id)?|member_?id)\s*=?\s*)'
        r'(([A-Z]{3}(\d|[A-Z]){8,12})'
        r'|(R(\d|[A-Z]){8,12}))(\s*|$)|'
        r'(\s*)RxBIN\s*=?\s*\d{6}(\s*|$)|'
        r'(\s*)RxPCN\s*=?\s*\[A-Za-z0-9]{1,10}(\s*|$)|'
        r'(\s*)Rx(Grp|\sGroup)\s*=?\s*[A-Za-z0-9\-]{1,15}(\s*|$)'
    )

    STANDALONE_CREDENTIALS = (
        r'(?<!(aws|aws_secret_access_key(?:_id)?|sha|github_token|github_access_token|github_token|'
        r'github_personal_access_token|github_sha|sha)\s*=\s*)(?<!\w)[0-9a-zA-Z/+]{40}(?!\w)',
        r'(?<!(aws|aws_access(?:_key|_key_id))\s*=\s*)(?<!\w)AKIA[0-9A-Z]{16}(\s*|$)',
        r'(?<!(aws_security_token|'
        r'aws_session_token)\s*=\s*)(?<!\w)[A-Za-z0-9+/]{342}\.[A-Za-z0-9+/]{4}\.[A-Za-z0-9+/]{43}(?!\w)'
        r'(?<!(azure_storage_account_key)\s*=\s*)(?<!\w)[A-Za-z0-9+/]{86}==|[A-Za-z0-9+/]{87}=|[A-Za-z0-9+/]{88}(?!\w)',
        r'(?<!auth0_client_id\s*=\s*)(?<!\w)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?!\w)',
        r'(?<!stripe_secret\s*=\s*)(?<!\w)[a-zA-Z0-9]{24}\.[a-zA-Z0-9]{32}(?!\w)',
        r'(?<!stripe_public_key\s*=\s*)(?<!\w)pk_test_[a-zA-Z0-9]{24}(?!\w)',
        r'(?<!twilio_account_sid\s*=\s*)(?<!\w)AC[a-zA-Z0-9]{32}(?!\w)',
        r'(?<!(twilio_auth_token|twilio_auth(?:_token)?|cognitive_services_api_key|open_ai|open_ai_key|'
        r'open_ai_api_key)\s*=\s*)(?<!\w)[a-zA-Z0-9]{32}(?!\w)',
        r'(?<!celery_broker_url\s*=\s*)(?<!\w)'
        r'amqp[s]?://[a-zA-Z0-9_]+:[a-zA-Z0-9_]+@[a-zA-Z0-9_.]+:[0-9]+/[a-zA-Z0-9_]+(?!\w)',
        r'(?<!(send_grid_key|send_grid_pass(?:word))\s*=\s*)(?<!\w)SG\.[a-zA-Z0-9_]{22}\.[a-zA-Z0-9_]{43}(?!\w)',
        r'(?<!gcp_api_key\s*=\s*)(?<!\w)AIza[a-zA-Z0-9]{35}(?!\w)',
        r'(?<!private_key_id\s*=\s*)(?<!\w)[a-zA-Z0-9_-]{28}(?!\w)',
        r'(?<!client_id\s*=\s*)(?<!\w)4[0-9]{20}(?!\w)',
        r'(?<!auth_uri\s*=\s*)(?<!\w)https://accounts.google.com/o/oauth2/auth.*(?!\w)',
        r'(?<!token_uri\s*=\s*)(?<!\w)https://oauth2.googleapis.com/token.*(?!\w)',
        r'(?<!(client_x509_cert_url|auth_provider_x509_cert_url)\s*=\s*)(?<!\w)https://www\.googleapis\.com/.+(?!\w)',
        r'(?<!service_bus_sas_key\s*=\s*)(?<!\w)[a-zA-Z0-9~!@#$%^&*()-=_+{}\[\];:\'"\.<>?]{44}(?!\w)',
        r'(?<!(ssh-rsa|ssh-dsa|ssh-ecdsa|ssh-ed25519|ecdsa-sha2-nistp[0-9]{3})\s*=\s*)(?<!\w)'
        r'AAAA[0-9A-Za-z+/]+[=]{0,3}(?: [^@\s]+@[^@\s]+)?(\s*|$)(?!\w)',
        r'(?<!(ip(?:_v4)?(?:_address)?)\s*=\s*)(?<!\w)'
        r'(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.'
        r'(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.'
        r'(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.'
        r'(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(?!\w)',
        r'(?<!(ip(?:_v6)?(?:_address)?)\s*=\s*)(?<!\w)((([0-9a-fA-F]{1,4}:){6}|::(ffff(:0+:{1,4})?:){0,1}('
        r'([0-9a-fA-F]{1,4}:){0,6}))((25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})\.){3}'
        r'(25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})|([0-9a-fA-F]{1,4}:){1,7}:?([0-9a-fA-F]{1,4}:){0,7}[0-9a-fA-F]{1,4}'
        r'(?!\w))',
    )

    @property
    def credentials_patterns(self) -> dict[str, str]:
        return {
            'AWS_CREDENTIALS': self.AWS_CREDENTIALS,
            'AZURE_CREDENTIALS': self.AZURE_CREDENTIALS,
            'STRIPE_CREDENTIALS': self.STRIPE_CREDENTIALS,
            'SSH_KEYS': self.SSH_KEYS,
            'TWILIO_CREDENTIALS': self.TWILIO_CREDENTIALS,
            'CELERY_CREDENTIALS': self.CELERY_CREDENTIALS,
            'SENDGRID_CREDENTIALS': self.SENDGRID_CREDENTIALS,
            'GCP_CREDENTIALS': self.GCP_CREDENTIALS,
            'AUTH0_CREDENTIALS': self.AUTH0_CREDENTIALS,
            'SNOWFLAKE_CREDENTIALS': self.SNOWFLAKE_CREDENTIALS,
            'PRIVATE_CREDENTIALS': self.PRIVATE_CREDENTIALS,
            'OPENAI_KEY': self.OPEN_AI_KEY,
            'GITHUB_CREDENTIALS': self.GITHUB_CREDENTIALS,
            # 'CREDENTIAL': self.STANDALONE_CREDENTIALS,
            'IP_ADDRESSES': self.IP_ADDRESS,
            'INSURANCE_INFORMATION': self.INSURANCE_INFORMATION,
        }

    @property
    def default_patterns(self) -> dict[str, str]:
        return {
            'IN_PAN': self.IN_PAN,
            'IN_AADHAR': self.IN_AADHAR,
            'CREDIT_CARD': self.CREDIT_CARD,
            'EMAIL_ADDRESS': self.EMAIL_ADDRESS,
            'IBAN_CODE': self.IBAN_CODE,
            'CRYPTO': self.CRYPTO,
            'US_SSN': self.US_SSN,
            'UK_NHS': self.UK_NHS,
            'US_ITIN': self.US_ITIN,
            'US_PASSPORT': self.US_PASSPORT,
            'US_DRIVER_LICENSE': self.US_DRIVER_LICENSE,
            'MEDICAL_LICENSE': self.MEDICAL_LICENSE,
            'US_BANK_NUMBER': self.US_BANK_NUMBER,
            # 'NRP': self.NRP,
        }

    @property
    def system_entities(self) -> list[str]:
        # todo add customers_email & person
        return list(self.default_patterns.keys()) + list(self.credentials_patterns.keys())

regex = Regexes()
