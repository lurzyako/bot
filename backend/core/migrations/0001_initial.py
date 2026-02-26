# Generated manually for initial schema.
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="AdItem",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("ad_id", models.CharField(db_index=True, max_length=128, unique=True)),
                (
                    "source_type",
                    models.CharField(
                        choices=[("excel", "Excel"), ("manual", "Ручное")],
                        default="manual",
                        max_length=16,
                    ),
                ),
                ("external_id", models.CharField(blank=True, max_length=128)),
                ("title", models.CharField(max_length=512)),
                ("category", models.CharField(blank=True, max_length=64)),
                ("price", models.BigIntegerField(default=0)),
                ("year", models.PositiveIntegerField(blank=True, null=True)),
                ("details", models.TextField(blank=True)),
                ("location", models.CharField(blank=True, max_length=255)),
                ("image", models.TextField(blank=True)),
                (
                    "status",
                    models.CharField(
                        choices=[("active", "Активно"), ("inactive", "Неактивно"), ("archived", "Архив")],
                        default="active",
                        max_length=16,
                    ),
                ),
                ("author_telegram_id", models.BigIntegerField(blank=True, db_index=True, null=True)),
                ("author_username", models.CharField(blank=True, max_length=255)),
                ("author_first_name", models.CharField(blank=True, max_length=255)),
                ("author_last_name", models.CharField(blank=True, max_length=255)),
                ("created_at_remote", models.DateTimeField(blank=True, null=True)),
                ("raw_payload", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Объявление",
                "verbose_name_plural": "Объявления",
                "ordering": ["-updated_at"],
            },
        ),
        migrations.CreateModel(
            name="TelegramUser",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("telegram_id", models.BigIntegerField(db_index=True, unique=True)),
                ("username", models.CharField(blank=True, max_length=255)),
                ("first_name", models.CharField(blank=True, max_length=255)),
                ("last_name", models.CharField(blank=True, max_length=255)),
                ("language_code", models.CharField(blank=True, max_length=16)),
                ("phone_number", models.CharField(blank=True, max_length=32)),
                ("avatar_file_id", models.CharField(blank=True, max_length=255)),
                (
                    "role",
                    models.CharField(
                        choices=[
                            ("user", "Пользователь"),
                            ("leasing_company", "Лизинговая компания"),
                            ("admin", "Администратор"),
                        ],
                        default="user",
                        max_length=32,
                    ),
                ),
                ("is_authenticated", models.BooleanField(default=False)),
                ("authenticated_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Пользователь Telegram",
                "verbose_name_plural": "Пользователи Telegram",
                "ordering": ["-updated_at"],
            },
        ),
        migrations.CreateModel(
            name="UserAction",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("telegram_id", models.BigIntegerField(db_index=True)),
                ("username", models.CharField(blank=True, max_length=255)),
                ("first_name", models.CharField(blank=True, max_length=255)),
                ("last_name", models.CharField(blank=True, max_length=255)),
                ("action", models.CharField(db_index=True, max_length=128)),
                ("details", models.TextField(blank=True)),
                ("created_at", models.DateTimeField(db_index=True, default=django.utils.timezone.now)),
                ("raw_payload", models.JSONField(blank=True, default=dict)),
                (
                    "user",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="actions",
                        to="core.telegramuser",
                    ),
                ),
            ],
            options={
                "verbose_name": "Действие пользователя",
                "verbose_name_plural": "Действия пользователей",
                "ordering": ["-created_at"],
            },
        ),
    ]
