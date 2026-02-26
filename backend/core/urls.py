from django.urls import path

from . import views

urlpatterns = [
    path("health/", views.health, name="health"),
    path("users/upsert/", views.upsert_user, name="upsert_user"),
    path("users/<int:telegram_id>/role/", views.user_role, name="user_role"),
    path("actions/", views.create_action, name="create_action"),
    path("ads/upsert/", views.upsert_ad, name="upsert_ad"),
    path("ads/bulk-upsert/", views.bulk_upsert_ads, name="bulk_upsert_ads"),
    path("ads/update/", views.update_ad_with_permissions, name="update_ad_with_permissions"),
    path("ads/delete/", views.delete_ad_with_permissions, name="delete_ad_with_permissions"),
]
