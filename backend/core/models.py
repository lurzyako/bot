from django.db import models
from django.utils import timezone


class TelegramUser(models.Model):
    ROLE_USER = "user"
    ROLE_LEASING_COMPANY = "leasing_company"
    ROLE_ADMIN = "admin"

    ROLE_CHOICES = [
        (ROLE_USER, "Пользователь"),
        (ROLE_LEASING_COMPANY, "Лизинговая компания"),
        (ROLE_ADMIN, "Администратор"),
    ]

    telegram_id = models.BigIntegerField(unique=True, db_index=True)
    username = models.CharField(max_length=255, blank=True)
    first_name = models.CharField(max_length=255, blank=True)
    last_name = models.CharField(max_length=255, blank=True)
    language_code = models.CharField(max_length=16, blank=True)
    phone_number = models.CharField(max_length=32, blank=True)
    avatar_file_id = models.CharField(max_length=255, blank=True)
    role = models.CharField(max_length=32, choices=ROLE_CHOICES, default=ROLE_USER)
    is_authenticated = models.BooleanField(default=False)
    authenticated_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at"]
        verbose_name = "Пользователь Telegram"
        verbose_name_plural = "Пользователи Telegram"

    def __str__(self) -> str:
        username = f"@{self.username}" if self.username else "без username"
        return f"{self.telegram_id} ({username})"


class UserAction(models.Model):
    user = models.ForeignKey(
        TelegramUser,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="actions",
    )
    telegram_id = models.BigIntegerField(db_index=True)
    username = models.CharField(max_length=255, blank=True)
    first_name = models.CharField(max_length=255, blank=True)
    last_name = models.CharField(max_length=255, blank=True)
    action = models.CharField(max_length=128, db_index=True)
    details = models.TextField(blank=True)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    raw_payload = models.JSONField(default=dict, blank=True)

    class Meta:
        ordering = ["-created_at"]
        verbose_name = "Действие пользователя"
        verbose_name_plural = "Действия пользователей"

    def __str__(self) -> str:
        return f"{self.telegram_id}: {self.action}"


class AdItem(models.Model):
    SOURCE_EXCEL = "excel"
    SOURCE_MANUAL = "manual"

    SOURCE_CHOICES = [
        (SOURCE_EXCEL, "Excel"),
        (SOURCE_MANUAL, "Ручное"),
    ]

    STATUS_ACTIVE = "active"
    STATUS_INACTIVE = "inactive"
    STATUS_ARCHIVED = "archived"

    STATUS_CHOICES = [
        (STATUS_ACTIVE, "Активно"),
        (STATUS_INACTIVE, "Неактивно"),
        (STATUS_ARCHIVED, "Архив"),
    ]

    ad_id = models.CharField(max_length=128, unique=True, db_index=True)
    source_type = models.CharField(max_length=16, choices=SOURCE_CHOICES, default=SOURCE_MANUAL)
    external_id = models.CharField(max_length=128, blank=True)
    title = models.CharField(max_length=512)
    category = models.CharField(max_length=64, blank=True)
    price = models.BigIntegerField(default=0)
    year = models.PositiveIntegerField(null=True, blank=True)
    details = models.TextField(blank=True)
    location = models.CharField(max_length=255, blank=True)
    image = models.TextField(blank=True)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_ACTIVE)

    author_telegram_id = models.BigIntegerField(null=True, blank=True, db_index=True)
    author_username = models.CharField(max_length=255, blank=True)
    author_first_name = models.CharField(max_length=255, blank=True)
    author_last_name = models.CharField(max_length=255, blank=True)

    created_at_remote = models.DateTimeField(null=True, blank=True)
    raw_payload = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at"]
        verbose_name = "Объявление"
        verbose_name_plural = "Объявления"

    def __str__(self) -> str:
        return f"{self.ad_id}: {self.title[:60]}"
