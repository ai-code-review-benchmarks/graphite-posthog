from inline_snapshot import snapshot

from posthog.cdp.templates.accoil.template_accoil import template as template_accoil
from posthog.cdp.templates.helpers import BaseHogFunctionTemplateTest


def create_inputs(**kwargs):
    inputs = {
        "apiKey": "test-api-key-123",
        "timestamp": "2024-10-24T23:03:50.941Z",
        "userId": "user123",
        "anonymousId": "",
        "event": "custom_event",
        "name": "Test Page",
        "email": "test@example.com",
        "user_name": "Test User",
        "role": "",
        "accountStatus": "",
        "createdAt": "",
        "user_traits_mapping": {},
        "groupId": "group_123",
        "group_name": "Test Group",
        "group_createdAt": "",
        "group_status": "active",
        "group_plan": "premium",
        "group_mrr": 100.0,
        "group_traits_mapping": {},
    }
    inputs.update(kwargs)
    return inputs


class TestTemplateAccoil(BaseHogFunctionTemplateTest):
    template = template_accoil

    def test_track_call(self):
        self.run_function(
            inputs=create_inputs(event="button_click"),
            globals={
                "event": {
                    "event": "button_click",
                    "uuid": "test-uuid-123",
                    "distinct_id": "user123",
                    "timestamp": "2024-10-24T23:03:50.941Z",
                    "properties": {
                        "button_id": "submit",
                        "page": "checkout",
                    },
                },
                "person": {
                    "properties": {
                        "email": "test@example.com",
                        "name": "Test User",
                    }
                },
            },
        )

        assert self.get_mock_fetch_calls()[0] == snapshot(
            (
                "https://in.accoil.com/segment",
                {
                    "method": "POST",
                    "headers": {
                        "Authorization": "Basic dGVzdC1hcGkta2V5LTEyMzo=",
                        "Content-Type": "application/json",
                    },
                    "body": {
                        "type": "track",
                        "timestamp": "2024-10-24T23:03:50.941Z",
                        "userId": "user123",
                        "event": "button_click",
                    },
                },
            )
        )

    def test_page_call(self):
        self.run_function(
            inputs=create_inputs(name="Home Page"),
            globals={
                "event": {
                    "event": "$pageview",
                    "uuid": "test-uuid-123",
                    "distinct_id": "user123",
                    "timestamp": "2024-10-24T23:03:50.941Z",
                    "properties": {
                        "title": "Home Page",
                        "$pathname": "/home",
                    },
                },
                "person": {
                    "properties": {
                        "email": "test@example.com",
                        "name": "Test User",
                    }
                },
            },
        )

        assert self.get_mock_fetch_calls()[0] == snapshot(
            (
                "https://in.accoil.com/segment",
                {
                    "method": "POST",
                    "headers": {
                        "Authorization": "Basic dGVzdC1hcGkta2V5LTEyMzo=",
                        "Content-Type": "application/json",
                    },
                    "body": {
                        "type": "page",
                        "timestamp": "2024-10-24T23:03:50.941Z",
                        "userId": "user123",
                        "name": "Home Page",
                    },
                },
            )
        )

    def test_screen_call(self):
        self.run_function(
            inputs=create_inputs(name="Settings Screen"),
            globals={
                "event": {
                    "event": "$screen",
                    "uuid": "test-uuid-123",
                    "distinct_id": "user123",
                    "timestamp": "2024-10-24T23:03:50.941Z",
                    "properties": {
                        "$screen_name": "Settings",
                    },
                },
                "person": {
                    "properties": {
                        "email": "test@example.com",
                        "name": "Test User",
                    }
                },
            },
        )

        assert self.get_mock_fetch_calls()[0] == snapshot(
            (
                "https://in.accoil.com/segment",
                {
                    "method": "POST",
                    "headers": {
                        "Authorization": "Basic dGVzdC1hcGkta2V5LTEyMzo=",
                        "Content-Type": "application/json",
                    },
                    "body": {
                        "type": "screen",
                        "timestamp": "2024-10-24T23:03:50.941Z",
                        "userId": "user123",
                        "name": "Settings Screen",
                    },
                },
            )
        )

    def test_identify_call(self):
        self.run_function(
            inputs=create_inputs(
                email="test@example.com",
                user_name="Test User",
                role="admin",
                accountStatus="active",
                createdAt="2023-01-01T00:00:00Z",
                user_traits_mapping={"company": "Acme Corp", "plan": "premium"},
            ),
            globals={
                "event": {
                    "event": "$identify",
                    "uuid": "test-uuid-123",
                    "distinct_id": "user123",
                    "timestamp": "2024-10-24T23:03:50.941Z",
                    "properties": {},
                },
                "person": {
                    "properties": {
                        "email": "test@example.com",
                        "name": "Test User",
                    }
                },
            },
        )

        assert self.get_mock_fetch_calls()[0] == snapshot(
            (
                "https://in.accoil.com/segment",
                {
                    "method": "POST",
                    "headers": {
                        "Authorization": "Basic dGVzdC1hcGkta2V5LTEyMzo=",
                        "Content-Type": "application/json",
                    },
                    "body": {
                        "type": "identify",
                        "timestamp": "2024-10-24T23:03:50.941Z",
                        "userId": "user123",
                        "traits": {
                            "email": "test@example.com",
                            "name": "Test User",
                            "role": "admin",
                            "accountStatus": "active",
                            "createdAt": "2023-01-01T00:00:00Z",
                            "company": "Acme Corp",
                            "plan": "premium",
                        },
                    },
                },
            )
        )

    def test_group_call(self):
        self.run_function(
            inputs=create_inputs(
                groupId="group_456",
                group_name="Acme Corp",
                group_status="active",
                group_plan="enterprise",
                group_mrr="500.0",
                group_createdAt="2023-01-01T00:00:00Z",
                group_traits_mapping={"industry": "Software", "employees": 100},
            ),
            globals={
                "event": {
                    "event": "$groupidentify",
                    "uuid": "test-uuid-123",
                    "distinct_id": "user123",
                    "timestamp": "2024-10-24T23:03:50.941Z",
                    "properties": {
                        "$group_key": "group_456",
                        "$group_set": {
                            "name": "Acme Corp",
                        },
                    },
                },
                "person": {
                    "properties": {
                        "email": "test@example.com",
                        "name": "Test User",
                    }
                },
            },
        )

        assert self.get_mock_fetch_calls()[0] == snapshot(
            (
                "https://in.accoil.com/segment",
                {
                    "method": "POST",
                    "headers": {
                        "Authorization": "Basic dGVzdC1hcGkta2V5LTEyMzo=",
                        "Content-Type": "application/json",
                    },
                    "body": {
                        "type": "group",
                        "timestamp": "2024-10-24T23:03:50.941Z",
                        "userId": "user123",
                        "groupId": "group_456",
                        "traits": {
                            "name": "Acme Corp",
                            "createdAt": "2023-01-01T00:00:00Z",
                            "status": "active",
                            "plan": "enterprise",
                            "mrr": 500.0,
                            "industry": "Software",
                            "employees": 100,
                        },
                    },
                },
            )
        )

    def test_anonymous_user_with_anon_id(self):
        self.run_function(
            inputs=create_inputs(
                userId="user789",
                anonymousId="anon123",
            ),
            globals={
                "event": {
                    "event": "page_view",
                    "uuid": "test-uuid-456",
                    "distinct_id": "user789",
                    "timestamp": "2024-10-24T23:03:50.941Z",
                    "properties": {
                        "$anon_distinct_id": "anon123",
                    },
                },
                "person": {"properties": {}},
            },
        )

        assert self.get_mock_fetch_calls()[0] == snapshot(
            (
                "https://in.accoil.com/segment",
                {
                    "method": "POST",
                    "headers": {
                        "Authorization": "Basic dGVzdC1hcGkta2V5LTEyMzo=",
                        "Content-Type": "application/json",
                    },
                    "body": {
                        "type": "track",
                        "timestamp": "2024-10-24T23:03:50.941Z",
                        "userId": "user789",
                        "anonymousId": "anon123",
                        "event": "custom_event",
                    },
                },
            )
        )

    def test_staging_api_key(self):
        self.run_function(
            inputs=create_inputs(
                apiKey="stg_test_api_key_456",
                event="test_event",
            ),
            globals={
                "event": {
                    "event": "test_event",
                    "uuid": "test-uuid-789",
                    "distinct_id": "user456",
                    "timestamp": "2024-10-24T23:03:50.941Z",
                    "properties": {},
                },
                "person": {"properties": {}},
            },
        )

        assert self.get_mock_fetch_calls()[0] == snapshot(
            (
                "https://instaging.accoil.com/segment",
                {
                    "method": "POST",
                    "headers": {
                        "Authorization": "Basic c3RnX3Rlc3RfYXBpX2tleV80NTY6",
                        "Content-Type": "application/json",
                    },
                    "body": {
                        "type": "track",
                        "timestamp": "2024-10-24T23:03:50.941Z",
                        "userId": "user123",
                        "event": "test_event",
                    },
                },
            )
        )

    def test_staging_api_key_case_insensitive(self):
        self.run_function(
            inputs=create_inputs(
                apiKey="STG_TEST_API_KEY_789",
                event="test_event",
            ),
            globals={
                "event": {
                    "event": "test_event",
                    "uuid": "test-uuid-999",
                    "distinct_id": "user789",
                    "timestamp": "2024-10-24T23:03:50.941Z",
                    "properties": {},
                },
                "person": {"properties": {}},
            },
        )

        assert self.get_mock_fetch_calls()[0] == snapshot(
            (
                "https://instaging.accoil.com/segment",
                {
                    "method": "POST",
                    "headers": {
                        "Authorization": "Basic U1RHX1RFU1RfQVBJX0tFWV83ODk6",
                        "Content-Type": "application/json",
                    },
                    "body": {
                        "type": "track",
                        "timestamp": "2024-10-24T23:03:50.941Z",
                        "userId": "user123",
                        "event": "test_event",
                    },
                },
            )
        )
