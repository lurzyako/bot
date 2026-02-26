from django.contrib import admin

from .models import AdItem, TelegramUser, UserAction


@admin.register(TelegramUser)
class TelegramUserAdmin(admin.ModelAdmin):
    list_display = (
        "telegram_id",
        "username",
        "first_name",
        "last_name",
        "role",
        "is_authenticated",
        "updated_at",
    )
    list_filter = ("role", "is_authenticated")
    search_fields = ("telegram_id", "username", "first_name", "last_name", "phone_number")
    readonly_fields = ("created_at", "updated_at")


@admin.register(UserAction)
class UserActionAdmin(admin.ModelAdmin):
    list_display = ("telegram_id", "action", "created_at")
    list_filter = ("action", "created_at")
    search_fields = ("telegram_id", "username", "first_name", "last_name", "details")
    readonly_fields = ("created_at",)


@admin.register(AdItem)
class AdItemAdmin(admin.ModelAdmin):
    list_display = ("ad_id", "title", "source_type", "category", "price", "status", "updated_at")
    list_filter = ("source_type", "status", "category")
    search_fields = ("ad_id", "external_id", "title", "location", "author_username")
    readonly_fields = ("created_at", "updated_at")
