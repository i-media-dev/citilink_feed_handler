import pytest
import sys
import os

from unittest.mock import Mock

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))


@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch):
    monkeypatch.setenv('XML_FEED_USERNAME', 'test_user')
    monkeypatch.setenv('XML_FEED_PASSWORD', 'test_pass')


@pytest.fixture
def sample_feeds():
    """Фикстура с тестовыми фидами."""
    return [
        'https://example.com/feed1.xml',
        'https://example.com/feed2.xml'
    ]


@pytest.fixture
def sample_xml_content():
    """Фикстура с валидным XML контентом."""
    return b'''<?xml version="1.0" encoding="UTF-8"?>
    <root>
        <item>Test</item>
    </root>'''


@pytest.fixture
def mock_response(sample_xml_content):
    """Фикстура с моком ответа requests."""
    mock = Mock()
    mock.status_code = 200
    mock.content = sample_xml_content
    return mock
