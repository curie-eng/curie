"""Control composition plumbing; no campaign acceptance or model qualification."""

from __future__ import annotations

import asyncio
import importlib

import pytest
from curie_protected_hooks.authority_records import parse_manifest
from curie_worker.commissioning_probe_registry import ProbeRegistry
from curie_worker.protected_launch_documents import LaunchControlUnavailable

from .protected_configuration_vectors import canonical
from .test_commissioning_control_profile import profile, profile_value, projection
from .test_commissioning_probe_runtime import ASSIGNMENT, Api, Primitive, pod_name
from .test_commissioning_worker_entry import _name

vectors = importlib.import_module(_name + ".test_commissioning_provisioner")


def product():
    from curie_worker import commissioning_control_process

    return commissioning_control_process


def binding():
    p = projection()
    built = vectors.built(admission_policy_sha256=profile().digest)
    m = built.manifest.as_dict()
    m["substrate"].update(
        authority_domain_id=p.as_dict()["authority_domain_id"],
        launch_identity=p.as_dict()["launch_identity"],
        launch_config_sha256=p.digest,
    )
    m["bundle_digest"] = {k: v for k, v in p.as_dict()["embedded_bundle"].items() if k != "path"}
    return product().ControlBinding(
        bootstrap=built.bootstrap,
        manifest=parse_manifest(canonical(m)),
        guard_config=built.guard_config,
        profile=profile(),
        projection=p,
    )


def test_binding_requires_the_parsed_g_profile_and_b_g_m_chain():
    api = product()
    bound = binding()
    assert bound.manifest.as_dict()["substrate"]["launch_config_sha256"] == bound.projection.digest
    kwargs = dict(
        bootstrap=bound.bootstrap,
        manifest=bound.manifest,
        guard_config=bound.guard_config,
        profile=bound.profile,
        projection=bound.projection,
    )
    value = profile_value()
    value["guard"]["timeout_seconds"] = 4
    with pytest.raises(api.ControlProcessRefused):
        api.ControlBinding(**(kwargs | {"profile": profile(value)}))
    m = bound.manifest.as_dict()
    m["guard_identity"]["config_sha256"] = "a" * 64
    with pytest.raises(api.ControlProcessRefused):
        api.ControlBinding(**(kwargs | {"manifest": parse_manifest(canonical(m))}))
    m = bound.manifest.as_dict()
    m["substrate"]["launch_config_sha256"] = "b" * 64
    with pytest.raises(api.ControlProcessRefused):
        api.ControlBinding(**(kwargs | {"manifest": parse_manifest(canonical(m))}))
    with pytest.raises(api.ControlProcessRefused):
        api.ControlBinding(**(kwargs | {"guard_config": b"{}"}))


def test_probe_restart_retires_previous_workload_then_uses_new_registry_and_ids(tmp_path):
    async def scenario():
        api = product()
        kube = Api()
        root = tmp_path / "probes"
        root.mkdir(mode=0o700)
        first = await api.ProbeBoot.open(root, binding=binding(), kubernetes=kube, count=1)
        old_root, old_id = first.registry_root, first.probe_ids[0]
        await first.runtime.launch(old_id, ASSIGNMENT)
        kube.run(pod_name(old_id))
        await first.runtime.exercise(old_id, Primitive())
        first.release()  # process death: durable rows survive, no fabricated cleanup
        second = await api.ProbeBoot.open(root, binding=binding(), kubernetes=kube, count=1)
        try:
            assert second.registry_root != old_root
            assert old_id not in second.probe_ids
            assert not kube.objects
            assert second.runtime.registry.probes() == ()
            with pytest.raises(LaunchControlUnavailable):
                await second.completions.observe()
            previous = ProbeRegistry.open(
                str(old_root), boot_id=second.boot_id, scope_digest=binding().profile.digest
            )
            try:
                assert previous.get(old_id).state == "retired"
            finally:
                previous.close()
        finally:
            assert await second.close()

    asyncio.run(scenario())


def test_uncertain_previous_boot_blocks_a_new_boot_without_ignoring_ownership(tmp_path):
    async def scenario():
        api = product()
        kube = Api()
        root = tmp_path / "probes"
        root.mkdir(mode=0o700)
        first = await api.ProbeBoot.open(root, binding=binding(), kubernetes=kube, count=1)
        kube.answer_create = False
        with pytest.raises(LaunchControlUnavailable):
            await first.runtime.launch(first.probe_ids[0], ASSIGNMENT)
        first.release()
        before = set(root.iterdir())
        with pytest.raises(api.ControlProcessRefused):
            await api.ProbeBoot.open(root, binding=binding(), kubernetes=kube, count=1)
        assert set(root.iterdir()) == before
        assert kube.objects  # unknown UID is preserved for independent recovery

    asyncio.run(scenario())


def test_only_one_process_owns_probe_generations_and_unrecognized_files_refuse(tmp_path):
    async def scenario():
        api = product()
        root = tmp_path / "probes"
        root.mkdir(mode=0o700)
        first = await api.ProbeBoot.open(root, binding=binding(), kubernetes=Api(), count=1)
        try:
            with pytest.raises(api.ControlProcessRefused):
                await api.ProbeBoot.open(root, binding=binding(), kubernetes=Api(), count=1)
        finally:
            await first.close()
        (root / "unknown").write_text("unknown ownership")
        with pytest.raises(api.ControlProcessRefused):
            await api.ProbeBoot.open(root, binding=binding(), kubernetes=Api(), count=1)

    asyncio.run(scenario())


def test_bounded_producer_rereads_and_refuses_a_hanging_external_read(tmp_path, monkeypatch):
    async def scenario():
        api = product()
        kube = Api()
        root = tmp_path / "probes"
        root.mkdir(mode=0o700)
        boot = await api.ProbeBoot.open(root, binding=binding(), kubernetes=kube, count=1)
        try:
            probe_id = boot.probe_ids[0]
            await boot.runtime.launch(probe_id, ASSIGNMENT)
            kube.run(pod_name(probe_id))
            await boot.runtime.exercise(probe_id, Primitive())
            assert await boot.runtime.retire_all()
            bounded = api.BoundedProducer(boot.completions)
            assert await bounded.observe() == await bounded.observe()
            kube.objects[("services", "pl-s-" + probe_id.replace("-", ""))] = {"metadata": {}}
            with pytest.raises(api.ControlProcessRefused):
                await bounded.observe()
            kube.objects.clear()

            async def hanging(*args):
                await asyncio.Event().wait()

            monkeypatch.setattr(kube, "read", hanging)
            monkeypatch.setitem(api.PRODUCER_CONSTANTS, "per_call_deadline_ms", 20)
            with pytest.raises(api.ControlProcessRefused):
                await asyncio.wait_for(bounded.observe(), 1)

            delayed = False

            async def late_absence(*args):
                nonlocal delayed
                if not delayed:
                    delayed = True
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        pass
                return None

            monkeypatch.setattr(kube, "read", late_absence)
            with pytest.raises(api.ControlProcessRefused):
                await asyncio.wait_for(bounded.observe(), 1)
        finally:
            boot.release()

    asyncio.run(scenario())


# The lifecycle uses the same owned TLS Valkey fixture as the commissioning
# worker; no journal, transport or broker is replaced by an in-memory fake.
from .test_commissioning_worker_entry import campaign, pworker_service  # noqa: E402

__all__ = ["campaign", "pworker_service"]


class JournalView:
    def __init__(self, journal):
        self.journal = journal
        self.expires_at_ms = int(journal.permit.as_dict()["expires_at_ms"])

    async def status(self):
        return await self.journal.status()

    async def begin_close(self):
        state = await self.journal.status()
        if state.state in {"running", "prepared"}:
            await self.journal.begin_close(state.generation)


class ProbeLaunches:
    """An owned external workload; use the actual runtime's UID/finalizer cleanup."""

    def __init__(self, probes):
        self.probes = probes
        self.fenced = False

    def fence(self):
        self.fenced = True

    async def close_campaign(self):
        await self.probes.runtime.retire_all()
        return tuple(
            row.probe_id for row in self.probes.runtime.registry.probes() if row.state != "retired"
        )


@pytest.mark.parametrize("ending", ["return", "error", "cancel", "expiry"])
def test_process_joins_and_retires_on_every_exit_with_real_journal(tmp_path, campaign, ending):
    async def scenario():
        api = product()
        kube = Api()
        root = tmp_path / "probes"
        root.mkdir(mode=0o700)
        probes = await api.ProbeBoot.open(root, binding=binding(), kubernetes=kube, count=1)
        await campaign.issue()
        journal = await campaign.journal()
        launches = ProbeLaunches(probes)
        process = api.ControlProcess(
            campaign=JournalView(journal),
            launches=launches,
            probes=probes,
            interval_s=0.01,
            join_s=1,
            close_s=1,
        )
        supervisor = process.supervisor
        running = asyncio.Event()
        joined = asyncio.Event()

        async def driver():
            probe_id = probes.probe_ids[0]
            await probes.runtime.launch(probe_id, ASSIGNMENT)
            kube.run(pod_name(probe_id))
            await probes.runtime.observe_runner(probe_id)
            running.set()
            try:
                if ending == "return":
                    return
                if ending == "error":
                    raise RuntimeError("private producer detail")
                await asyncio.Event().wait()
            finally:
                joined.set()

        task = asyncio.create_task(process.run(driver))
        try:
            await asyncio.wait_for(running.wait(), 5)
            if ending == "cancel":
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            elif ending == "expiry":
                # The real supervisor's independent wall clock observes expiry;
                # the broker remains real and records the closing transition.
                supervisor._clock = lambda: int(campaign.value["expires_at_ms"]) / 1000 + 1
                with pytest.raises(api.ControlProcessRefused):
                    await asyncio.wait_for(task, 5)
            elif ending == "error":
                with pytest.raises(
                    api.ControlProcessRefused, match="^Commissioning control process refused$"
                ):
                    await asyncio.wait_for(task, 5)
            else:
                report = await asyncio.wait_for(task, 5)
                assert report.complete
            assert launches.fenced and joined.is_set() and not kube.objects
            assert process.close_report is not None and process.close_report.complete
            assert (await journal.status()).state == "closing"
            with pytest.raises(api.ControlProcessRefused):
                await process.run(driver)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await journal.close()
            probes.release()

    asyncio.run(scenario())


def test_synchronous_driver_failure_still_stops_supervisor_and_releases_probe_lock(
    tmp_path, campaign
):
    async def scenario():
        api = product()
        root = tmp_path / "probes"
        root.mkdir(mode=0o700)
        probes = await api.ProbeBoot.open(root, binding=binding(), kubernetes=Api(), count=1)
        await campaign.issue()
        journal = await campaign.journal()
        launches = ProbeLaunches(probes)
        process = api.ControlProcess(
            campaign=JournalView(journal),
            launches=launches,
            probes=probes,
            interval_s=0.01,
            close_s=1,
        )
        supervisor = process.supervisor

        def broken():
            raise RuntimeError("private driver detail")

        try:
            with pytest.raises(api.ControlProcessRefused):
                await process.run(broken)
            assert launches.fenced and process.close_report.complete
            reopened = await api.ProbeBoot.open(root, binding=binding(), kubernetes=Api(), count=1)
            assert await reopened.close()
        finally:
            supervisor.revoke()
            probes.release()
            await journal.close()

    asyncio.run(scenario())


def test_repeated_cancellation_cannot_detach_cleanup(tmp_path, campaign):
    async def scenario():
        api = product()
        deleting, allow_delete = asyncio.Event(), asyncio.Event()

        class SlowDelete(Api):
            async def delete(self, *args):
                deleting.set()
                await allow_delete.wait()
                await super().delete(*args)

        kube = SlowDelete()
        root = tmp_path / "probes"
        root.mkdir(mode=0o700)
        probes = await api.ProbeBoot.open(root, binding=binding(), kubernetes=kube, count=1)
        await campaign.issue()
        journal = await campaign.journal()
        process = api.ControlProcess(
            campaign=JournalView(journal),
            launches=ProbeLaunches(probes),
            probes=probes,
            interval_s=0.01,
            close_s=1,
        )
        running = asyncio.Event()

        async def driver():
            await probes.runtime.launch(probes.probe_ids[0], ASSIGNMENT)
            kube.run(pod_name(probes.probe_ids[0]))
            await probes.runtime.observe_runner(probes.probe_ids[0])
            running.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(process.run(driver))
        try:
            await asyncio.wait_for(running.wait(), 5)
            task.cancel()
            await asyncio.wait_for(deleting.wait(), 5)
            task.cancel()
            await asyncio.sleep(0.01)
            assert not task.done()
            allow_delete.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 5)
            assert process.close_report.complete and not kube.objects
        finally:
            allow_delete.set()
            await asyncio.gather(task, return_exceptions=True)
            await journal.close()
            probes.release()

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", [0o777, 0o755, 0o750])
def test_probe_boot_refuses_nonprivate_root_before_creating_files(tmp_path, mode):
    async def scenario():
        api = product()
        root = tmp_path / "probes"
        root.mkdir(mode=0o700)
        root.chmod(mode)
        opened = None
        try:
            with pytest.raises(api.ControlProcessRefused):
                opened = await api.ProbeBoot.open(
                    root, binding=binding(), kubernetes=Api(), count=1
                )
            assert list(root.iterdir()) == []
        finally:
            if opened is not None:
                opened.release()

    asyncio.run(scenario())


@pytest.mark.parametrize("phase", ["status", "begin_close", "launches"])
def test_process_refuses_timed_out_external_cleanup(
    tmp_path, campaign, pworker_service, monkeypatch, phase
):
    async def scenario():
        api = product()
        monkeypatch.setitem(api.PRODUCER_CONSTANTS, "per_call_deadline_ms", 20)
        observed = asyncio.Event()

        class ObservedJournal(JournalView):
            async def status(self):
                state = await super().status()
                observed.set()
                return state

        class SlowDelete(Api):
            async def delete(self, *args):
                await asyncio.sleep(0.3)
                await super().delete(*args)

        kube = SlowDelete() if phase == "launches" else Api()
        root = tmp_path / "probes"
        root.mkdir(mode=0o700)
        probes = await api.ProbeBoot.open(root, binding=binding(), kubernetes=kube, count=1)
        await campaign.issue()
        journal = await campaign.journal()
        launches = ProbeLaunches(probes)
        process = api.ControlProcess(
            campaign=ObservedJournal(journal),
            launches=launches,
            probes=probes,
            interval_s=0.01,
            join_s=0.03,
            close_s=0.03,
        )

        async def driver():
            if phase == "status":
                await asyncio.Event().wait()
            await observed.wait()
            if phase == "begin_close":
                # Pause the actual owned TLS broker; no journal result is mocked.
                pworker_service.command("CLIENT", "PAUSE", 300, "ALL")
            else:
                await probes.runtime.launch(probes.probe_ids[0], ASSIGNMENT)
                kube.run(pod_name(probes.probe_ids[0]))
                await probes.runtime.observe_runner(probes.probe_ids[0])

        if phase == "status":
            pworker_service.command("CLIENT", "PAUSE", 300, "ALL")
        task = asyncio.create_task(process.run(driver))
        try:
            done, _ = await asyncio.wait((task,), timeout=1)
            assert task in done, "cleanup did not finish after bounded transport draining"
            with pytest.raises(api.ControlProcessRefused):
                await task
            assert process.close_report is not None and not process.close_report.complete
            assert launches.fenced
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await journal.close()
            probes.release()

    asyncio.run(scenario())


def test_producer_cannot_turn_suppressed_cancellation_into_a_late_digest(monkeypatch):
    api = product()
    monkeypatch.setitem(api.PRODUCER_CONSTANTS, "per_call_deadline_ms", 20)

    class LateProducer:
        # Deliberately malformed cancellation plumbing; no installation evidence.
        fact = "artifact"

        async def observe(self):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                return "a" * 64

    with pytest.raises(api.ControlProcessRefused):
        asyncio.run(api.BoundedProducer(LateProducer()).observe())


def test_unjoined_driver_refuses_and_reports_pending_ownership(tmp_path, campaign):
    async def scenario():
        api = product()
        root = tmp_path / "probes"
        root.mkdir(mode=0o700)
        probes = await api.ProbeBoot.open(root, binding=binding(), kubernetes=Api(), count=1)
        await campaign.issue()
        journal = await campaign.journal()
        process = api.ControlProcess(
            campaign=JournalView(journal),
            launches=ProbeLaunches(probes),
            probes=probes,
            interval_s=0.01,
            join_s=0.02,
            close_s=0.1,
        )
        running, finish = asyncio.Event(), asyncio.Event()
        owned = None

        async def driver():
            nonlocal owned
            owned = asyncio.current_task()
            running.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await finish.wait()

        task = asyncio.create_task(process.run(driver))
        try:
            await running.wait()
            process.supervisor.revoke()
            with pytest.raises(api.ControlProcessRefused):
                await asyncio.wait_for(task, 1)
            assert not process.close_report.complete
            assert not process.close_report.campaign.joined
            assert owned is not None and not owned.done()
        finally:
            finish.set()  # the outer owner joins the resistant session explicitly
            if owned is not None:
                await owned
            await asyncio.gather(task, return_exceptions=True)
            probes.release()
            await journal.close()

    asyncio.run(scenario())
