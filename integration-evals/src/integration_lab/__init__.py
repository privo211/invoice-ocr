"""Reliable API integration patterns, implemented against JSONPlaceholder."""

from .client import IntegrationClient
from .models import Post, User

__all__ = ["IntegrationClient", "Post", "User"]
