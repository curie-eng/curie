"""FastAPI dependencies that pull shared resources off app.state."""

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .approvers import ApproverSetSelector
from .evalqueue import EvalQueue
from .github_checks import GitHubStatusReporter
from .k8s import PodLister, PodLogReader
from .killswitch import KillSwitch
from .langfuse import LangfuseClient
from .resumequeue import ResumeQueue
from .storage import ObjectStore
from .threadreset import ThreadResetRequests


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    sessionmaker: async_sessionmaker[AsyncSession] = request.app.state.sessionmaker
    async with sessionmaker() as session:
        yield session


def get_langfuse(request: Request) -> LangfuseClient:
    client: LangfuseClient = request.app.state.langfuse
    return client


def get_store(request: Request) -> ObjectStore:
    store: ObjectStore = request.app.state.bundle_store
    return store


def get_pod_log_reader(request: Request) -> PodLogReader:
    reader: PodLogReader = request.app.state.pod_log_reader
    return reader


def get_pod_lister(request: Request) -> PodLister:
    lister: PodLister = request.app.state.pod_lister
    return lister


def get_kill_switch(request: Request) -> KillSwitch:
    kill_switch: KillSwitch = request.app.state.kill_switch
    return kill_switch


def get_eval_queue(request: Request) -> EvalQueue:
    eval_queue: EvalQueue = request.app.state.eval_queue
    return eval_queue


def get_github_reporter(request: Request) -> GitHubStatusReporter:
    reporter: GitHubStatusReporter = request.app.state.github_reporter
    return reporter


def get_resume_queue(request: Request) -> ResumeQueue:
    resume_queue: ResumeQueue = request.app.state.resume_queue
    return resume_queue


def get_thread_reset_requests(request: Request) -> ThreadResetRequests:
    requests: ThreadResetRequests = request.app.state.thread_reset_requests
    return requests


def get_approver_sets(request: Request) -> ApproverSetSelector:
    """Picks the approver set a route binding calls for (#420).

    Always present: which provider's selector was wired is main's decision, and
    a deployment with no bot token still selects sets, it just cannot resolve a
    group's membership when one is asked for."""

    selector: ApproverSetSelector = request.app.state.approver_sets
    return selector


SessionDep = Annotated[AsyncSession, Depends(get_session)]
LangfuseDep = Annotated[LangfuseClient, Depends(get_langfuse)]
StoreDep = Annotated[ObjectStore, Depends(get_store)]
PodLogReaderDep = Annotated[PodLogReader, Depends(get_pod_log_reader)]
PodListerDep = Annotated[PodLister, Depends(get_pod_lister)]
KillSwitchDep = Annotated[KillSwitch, Depends(get_kill_switch)]
EvalQueueDep = Annotated[EvalQueue, Depends(get_eval_queue)]
GitHubReporterDep = Annotated[GitHubStatusReporter, Depends(get_github_reporter)]
ResumeQueueDep = Annotated[ResumeQueue, Depends(get_resume_queue)]
ApproverSetSelectorDep = Annotated[ApproverSetSelector, Depends(get_approver_sets)]
ThreadResetRequestsDep = Annotated[ThreadResetRequests, Depends(get_thread_reset_requests)]
