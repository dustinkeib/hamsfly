import os
from pathlib import Path
from django.http import HttpResponseForbidden
from dotenv import load_dotenv

# Load .env from the project root (same directory as manage.py)
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path, override=True)


class AdminIPRestrictMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.path.startswith('/admin/'):
            # Get allowed IPs from env (read each request for flexibility)
            allowed = os.environ.get('ADMIN_ALLOWED_IPS', '')
            allowed_ips = [ip.strip() for ip in allowed.split(',') if ip.strip()]

            # Get client IP, checking X-Forwarded-For for proxies (Render)
            x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
            if x_forwarded_for:
                ip = x_forwarded_for.split(',')[0].strip()
            else:
                ip = request.META.get('REMOTE_ADDR')

            if allowed_ips and ip not in allowed_ips:
                return HttpResponseForbidden(f'Forbidden - IP: {ip}, allowed: {allowed_ips}')

        return self.get_response(request)
