from django.urls import path
from .views import JobDetailView, JobStatusView, JobListByStatusView, homepage, JobListCreateView

app_name = 'jobs'

urlpatterns = [
    path('', homepage, name='homepage'),
    path('api/jobs/', JobListCreateView.as_view(),name='job-list-create'),
    path('api/jobs/<int:pk>/', JobDetailView.as_view(),name='job-detail'),
    path('api/jobs/<int:pk>/status/', JobStatusView.as_view(),name='job-status'),
    path('api/jobs/status/<str:status>/',JobListByStatusView.as_view(),name='job-list-by-status'),
]
