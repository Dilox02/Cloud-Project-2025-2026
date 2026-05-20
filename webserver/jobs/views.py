import logging
from django.shortcuts import render, get_object_or_404
from rest_framework import generics, status
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import Job
from .serializers import JobSerializer, JobCreateSerializer, JobStatusSerializer
from .runner import launch_job

logger = logging.getLogger(__name__)

def homepage(request):
    """it shows the last 10 jobs"""
    jobs = Job.objects.all()[:10]
    return render(request, 'homepage.html', {'jobs': jobs})


class JobListCreateView(generics.ListCreateAPIView):
    """
    GET  /api/jobs/  -> it retrieves all the jobs paginated
    POST /api/jobs/  ->  it creates and submits a new job
    """
    queryset = Job.objects.all()

    def get_serializer_class(self):
        if self.request.method == 'POST':
            return JobCreateSerializer
        return JobSerializer

    def perform_create(self, serializer):
        job = serializer.save()
        logger.info(
            "new job #%d created — engine=%s, dataset=%s",
            job.id, job.engine, job.dataset_path,
        )
        launch_job(job.id)

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)
        # it returns the full job object
        full = JobSerializer(serializer.instance, context=self.get_serializer_context())
        headers = self.get_success_headers(full.data)
        return Response(full.data, status=status.HTTP_201_CREATED, headers=headers)


class JobDetailView(generics.RetrieveAPIView):
    """
    GET /api/jobs/<id>/ -> it retrieves the full job object
    """
    queryset = Job.objects.all()
    serializer_class = JobSerializer


class JobStatusView(APIView):
    """
    GET /api/jobs/<id>/status/ -> it retrieves the job status
    """

    def get(self, request, pk):
        job = get_object_or_404(Job, pk=pk)
        serializer = JobStatusSerializer(job)
        return Response(serializer.data)


class JobListByStatusView(generics.ListAPIView):
    """
    GET /api/jobs/status/<status>/ -> it filters jobs by status
    """
    serializer_class = JobSerializer

    def get_queryset(self):
        status = self.kwargs['status']
        valid = [s.value for s in Job.Status]
        if status not in valid:
            return Job.objects.none()
        return Job.objects.filter(status=status)