# Historical native execution loop. Tests only; never a product fallback.
# Extracted verbatim from v0.3.0 source, with the identities in legacy_native_runner_v0_3_0.json.
def _run_optimization_unleased(spec, config, prepared=None, run_root=None,
                               resume_checkpoint=None, continuation_id=None,
                               checkpoint_anchor_root=None):
    if resume_checkpoint is not None:
        if not isinstance(resume_checkpoint, dict) or resume_checkpoint.get("schema_version") != "crescendo.checkpoint.v1":
            raise ValueError("unsupported checkpoint; expected crescendo.checkpoint.v1")
        spec = dict(resume_checkpoint.get("spec") or {})
    else:
        spec = dict(spec)
    evaluator = str(spec.get("evaluator", "real")).strip().lower()
    if evaluator not in ("real", "mock"):
        raise ValueError("unsupported optimization evaluator: %s" % spec.get("evaluator"))
    spec["evaluator"] = evaluator
    config = copy.deepcopy(config)
    spec["simulator"] = selected_simulator(config, spec)
    config.setdefault("cadence", {})["simulator"] = spec["simulator"]
    if evaluator == "real" and spec["simulator"] == "alps":
        from .simulators import simulator_configuration_identity
        simulator_identity = simulator_configuration_identity(config, spec)
        if resume_checkpoint is not None and spec.get("simulator_execution_identity") != simulator_identity:
            raise ValueError("ALPS checkpoint executable/model/section/thread/result-reader identity changed; start a fresh optimization")
        spec["simulator_execution_identity"] = simulator_identity

    display_name = _validated_run_display_name(spec.get("display_name"))
    if display_name is None:
        spec.pop("display_name", None)
    else:
        spec["display_name"] = display_name
    spec["objectives"], spec["constraints"] = normalize_metric_definitions(
        spec.get("objectives", []), spec.get("constraints", []),
    )
    candidate_timeout = int(spec.get("candidate_timeout_seconds", config["runtime"].get("candidate_timeout_seconds", 600)))
    if candidate_timeout < 30:
        raise ValueError("candidate_timeout_seconds must be at least 30")
    spec["candidate_timeout_seconds"] = candidate_timeout
    try:
        cleanup_timeout = float(config["runtime"].get("persistent_cleanup_timeout_seconds", 20))
        commit_timeout = float(config["runtime"].get("controller_commit_timeout_seconds", 60))
        cleanup_termination_margin = float(config["runtime"].get(
            "persistent_cleanup_termination_margin_seconds", 20,
        ))
    except (TypeError, ValueError, OverflowError):
        raise ValueError("persistent cleanup and controller commit timeouts must be finite numbers")
    if not math.isfinite(cleanup_timeout) or cleanup_timeout <= 0:
        raise ValueError("persistent_cleanup_timeout_seconds must be finite and positive")
    if (
        not math.isfinite(cleanup_termination_margin)
        or cleanup_termination_margin < 0
    ):
        raise ValueError(
            "persistent_cleanup_termination_margin_seconds must be finite and non-negative"
        )
    if (
        not math.isfinite(commit_timeout)
        or commit_timeout <= cleanup_timeout + cleanup_termination_margin
    ):
        raise ValueError(
            "controller_commit_timeout_seconds must exceed the cleanup timeout plus termination margin"
        )
    variables = validate_variables(spec.get("variables", []))
    if not spec.get("objectives"):
        raise ValueError("at least one objective is required")
    objective_count = len(spec["objectives"])
    checkpoint_algorithm_policy = spec.get("algorithm_policy") if resume_checkpoint is not None else None
    algorithm, algorithm_policy, algorithm_parameters = _normalize_algorithm_settings(
        spec, len(variables), objective_count,
    )
    if checkpoint_algorithm_policy is not None:
        algorithm_policy = checkpoint_algorithm_policy
    generations = int(spec.get("generations", 10))
    population_explicit = "population" in spec or "population" in (spec.get("algorithm_parameters") or {})
    population = algorithm_parameters["population"]
    if generations < 1:
        raise ValueError("generations must be at least 1")
    spec["algorithm"] = algorithm
    spec["algorithm_policy"] = algorithm_policy
    spec["algorithm_parameters"] = algorithm_parameters
    spec["seed"] = algorithm_parameters["seed"]
    spec["population"] = population
    spec["population_policy"] = "explicit" if population_explicit else "automatic_by_variable_count"
    prepared_was_supplied = prepared is not None
    if prepared is None:
        setup_reference = normalize_setup_reference(
            spec.get("setup_locator") or spec.get("setup_path") or spec.get("maestro_path")
        )
        selection = ((resume_checkpoint or {}).get("manifest") or {}).get("cell_selection")
        if selection is None:
            selection = spec.get("cell_selection")
        scopes = cell_selection_scopes(selection)
        options = {"backend": setup_reference["backend"]}
        if scopes is not None:
            options["selected_cell_paths"] = scopes
        prepared = prepare_setup(setup_reference["path"], config, spec.get("max_depth", 32), **options)
    if prepared_was_supplied:
        external_guard = _reverify_prepared_external_environment(prepared, config)
        if not external_guard.get("ok"):
            raise ValueError(external_guard.get("error") or "external EDA environment changed")
    if resume_checkpoint is not None:
        validate_checkpoint(resume_checkpoint, prepared)
    if prepared.get("cell_selection") is not None:
        spec["cell_selection"] = copy.deepcopy(prepared["cell_selection"])
    setup_reference = prepared_setup_reference(prepared=prepared, spec=spec)
    if setup_reference.get("path"):
        spec["setup_locator"] = setup_reference
        spec["setup_path"] = setup_reference["path"]
    if setup_reference.get("backend") == "maestro" and setup_reference.get("path"):
        spec["maestro_path"] = setup_reference["path"]
    elif setup_reference.get("backend") != "maestro":
        spec.pop("maestro_path", None)
    selected = _selected_parameters(prepared["parameters"], variables)
    variables = validate_variables(_enforce_catalog_variable_types(variables, selected))
    requested_backend = str(spec.get("execution_backend") or config.get("runtime", {}).get("execution_backend", "legacy"))
    persistent_backend = _persistent_backend_selected(spec, selected, config, prepared=prepared)
    netlist_backend = bool(spec.get("evaluator", "real") == "real" and requested_backend == "private_netlist")
    setup_backend = (prepared.get("setup") or {}).get("backend") or "maestro"
    if evaluator == "real" and not netlist_backend:
        native_simulators = set((test.get("simulator") or "spectre").lower() for test in (prepared.get("setup") or {}).get("tests", []) if test.get("enabled", True))
        if native_simulators and native_simulators != {spec["simulator"]}:
            raise ValueError("Native Maestro/ADE setup uses " + ", ".join(sorted(native_simulators)) + "; selected " + spec["simulator"] + " requires its saved native integration, or Private Netlist with explicit model mapping")
        if setup_backend == "adexl" and spec["simulator"] == "alps":
            raise ValueError("ALPS native ADE XL adapter is unqualified; use a saved ALPS Maestro setup or Private Netlist")
    spec["execution_backend"] = (
        "private_netlist" if netlist_backend
        else "persistent_maestro" if persistent_backend
        else "adexl" if setup_backend == "adexl"
        else "legacy"
    )
    if not persistent_backend and not netlist_backend:
        spec["legacy_guardian_protocol"] = LEGACY_GUARDIAN_PROTOCOL
    if resume_checkpoint is not None:
        for row in checkpoint_candidates(resume_checkpoint):
            validate_candidate_values(row.get("values") or {}, variables)
    run_root = os.path.abspath(run_root or os.path.join(config["runtime"]["run_root"], optimization_run_name(spec.get("display_name"))))
    if evaluator == "real":
        from .storage_policy import validate_native_data_path
        validate_native_data_path(run_root)
    startup_preflight = startup_storage_preflight(
        run_root, checkpoint_anchor_root or config["runtime"].get("run_root"),
        config["runtime"].get("persistent_min_free_disk_mib", 1024),
        setup=prepared.get("setup"), hierarchy=prepared.get("hierarchy"), config=config,
        checkpoint=resume_checkpoint,
        workspace_required=optimization_workspace_required(spec, resume_checkpoint),
        parameters=selected,
    )
    ensure_dir(run_root)
    config = copy.deepcopy(config)
    if _environment_test_run(spec):
        # Only this new diagnostic entry is bounded to a single attempt. The
        # user's normal runtime configuration is an independent deep copy.
        config.setdefault("runtime", {})["infrastructure_retries"] = 0
    from .job_policy import normalize_job_overrides
    if "maestro_job_overrides" in spec:
        spec["maestro_job_overrides"] = normalize_job_overrides(spec["maestro_job_overrides"])
        config.setdefault("runtime", {})["maestro_job_overrides"] = copy.deepcopy(spec["maestro_job_overrides"])
    if persistent_backend:
        config.setdefault('runtime', {})['maestro_source_job_policy'] = copy.deepcopy(prepared.get('job_policy') or {})
    execution_plan = candidate_execution_plan(spec, prepared, config)
    config.setdefault("runtime", {})["native_cluster_stop_required"] = (
        execution_plan["native_cluster_stop_required"] or
        (persistent_backend and str(spec.get('evaluator') or '').lower() == 'real'
         and not (prepared.get('job_policy') or {}).get('available'))
    )
    batch_enabled = bool(persistent_backend and execution_plan["enabled"])
    config.setdefault("runtime", {})["run_root"] = run_root
    config["runtime"]["_run_display_name"] = display_name or os.path.basename(run_root)
    stop_path = os.path.join(run_root, "STOP")
    status_path = os.path.join(run_root, "status.json")
    spec["variables"] = variables
    spec["variable_labels"] = dict(
        (parameter["parameter_id"], "%s/%s" % (
            str(parameter.get("hierarchy_path") or "/").rstrip("/"),
            parameter.get("name") or parameter["parameter_id"],
        ))
        for parameter in selected
    )
    atomic_write_json(os.path.join(run_root, "spec.json"), spec)
    if spec.get("evaluator", "real") == "real":
        export_prepared = dict((key, copy.deepcopy(prepared[key])) for key in
                               ("setup", "hierarchy", "source_identity", "pdk") if key in prepared)
        export_prepared["parameters"] = copy.deepcopy(selected)
        atomic_write_json(os.path.join(run_root, "schematic_export_context.json"), {
            "schema_version": "crescendo.schematic_export_context.v1",
            "prepared": export_prepared, "config": config,
            "origin_run_id": os.path.basename(((resume_checkpoint or {}).get("manifest") or {}).get("origin_run_root") or "") or None,
        })
    all_rows = checkpoint_candidates(resume_checkpoint) if resume_checkpoint is not None else []
    if resume_checkpoint is not None and resume_checkpoint.get("candidate_store") is not None:
        resumed_dataset = dataset_from_reference(resume_checkpoint["candidate_store"],
                                                 resume_checkpoint.get("candidate_store_embedded"))
        all_rows = sorted([copy.deepcopy(row) for row in resumed_dataset["candidates"]
                           if row.get("status") in DATA_TERMINAL_STATES],
                          key=lambda row: (row["generation"], row["index"]))
    elif resume_checkpoint is not None:
        _seed_checkpoint_candidates(run_root, all_rows)
    data_store = RunDataStore(os.path.join(run_root, CORE_NAME), metadata={
        "run_id": os.path.basename(run_root), "display_name": display_name or os.path.basename(run_root),
        "spec": copy.deepcopy(spec), "parameters": copy.deepcopy(selected),
        "outputs": copy.deepcopy(prepared.get("outputs") or []),
        "source_identity": copy.deepcopy(prepared.get("source_identity")),
        "execution_backend": spec["execution_backend"],
        "simulator": spec["simulator"],
        "continuation_id": continuation_id,
    })
    if all_rows:
        data_store.commit_candidates(all_rows)
    planned_core_ids = set(row["candidate_id"] for row in all_rows)
    point_commit_queue = queue.Queue()
    status = {
        "schema_version": "crescendo.run_status.v1",
        "run_id": os.path.basename(run_root),
        "run_root": run_root,
        "display_name": display_name or os.path.basename(run_root),
        "continuation_id": continuation_id,
        "state": "running",
        "started_at": utc_timestamp(),
        "generation": 0,
        "total_generations": generations,
        "completed_candidates": len([row for row in all_rows if row["status"] == "completed"]),
        "failed_candidates": len([row for row in all_rows if row["status"] == "failed"]),
        "infrastructure_failed_candidates": len([row for row in all_rows if row["status"] == "infrastructure_failed"]),
        "total_candidates": generations * population + len([
            row for row in all_rows if row["status"] == "infrastructure_failed"
        ]),
        "message": "checkpoint restored" if resume_checkpoint is not None else "preparation complete",
        "main_log_file": os.path.join(run_root, "crescendo.log"),
        "algorithm": algorithm,
        "algorithm_parameters": algorithm_parameters,
        "execution_backend": spec["execution_backend"],
        "simulator": spec["simulator"],
        "legacy_guardian_protocol": spec.get("legacy_guardian_protocol"),
        **({"environment_test": spec["environment_test"]} if spec.get("environment_test") else {}),
        "checkpoint_revision": int(
            ((resume_checkpoint or {}).get("state") or {}).get("checkpoint_revision", 0)
        ),
        "startup_storage_preflight": startup_preflight,
        "resource_guard_restart_count": int(
            ((resume_checkpoint or {}).get("state") or {}).get(
                "resource_guard_restart_count", 0,
            )
        ),
        "resource_guard_recycles": copy.deepcopy(
            ((resume_checkpoint or {}).get("state") or {}).get(
                "resource_guard_recycles", [],
            )
        )[-20:],
    }
    if resume_checkpoint is not None:
        status["resumed_from"] = resume_checkpoint.get("created_at")
    atomic_write_json(status_path, status)
    generator = PopulationGenerator(
        variables, algorithm, algorithm_parameters["seed"], parameters=algorithm_parameters,
    )
    selection_history = []
    start_generation = 0
    active_plan = None
    checkpoint_ancestry = []
    checkpoint_parent_checksum = None
    resume_terminal_state = None
    resume_terminal_reason = None
    terminal_checkpoint_written = False
    if resume_checkpoint is not None:
        checkpoint_state = resume_checkpoint.get("state") or {}
        selection_history = list(checkpoint_state.get("selection_history") or [])
        start_generation = int(checkpoint_state.get("next_generation", 0))
        active_plan = _unpack_plan(checkpoint_state.get("active_plan"), [row["parameter_id"] for row in variables])
        generator_state = checkpoint_state.get("generator") or {}
        if generator_state.get("rng_state") is not None:
            _restore_generator(generator, generator_state, all_rows)
        checkpoint_ancestry = list(checkpoint_state.get("ancestor_payload_sha256") or [])
        checkpoint_parent_checksum = _checkpoint_checksum(resume_checkpoint)
        resume_terminal_state = checkpoint_state.get("terminal_state")
        resume_terminal_reason = checkpoint_state.get("terminal_reason")
        terminal_checkpoint_written = resume_terminal_state == "completed"
    stopped_reason = resume_terminal_reason if resume_terminal_state == "completed" else None
    execution_start_generation = generations if resume_terminal_state == "completed" else start_generation
    netlist_optimizer = None
    if netlist_backend:
        origin_root = ((resume_checkpoint or {}).get("manifest") or {}).get("origin_run_root")
        prior_roots = []
        if origin_root and os.path.abspath(origin_root) != run_root:
            if not os.path.isabs(origin_root) or os.path.realpath(origin_root) != origin_root:
                raise ValueError("private netlist checkpoint origin is not an independent absolute run directory")
            prior_roots.append({
                "owned_run_root": origin_root,
                "ownership_root": os.path.join(origin_root, "private_netlist", "execution_ownership"),
            })
        netlist_optimizer = PrivateNetlistOptimizer(
            run_root, spec, prepared, selected, config, _failure_metrics,
            cancel_check=lambda: os.path.exists(stop_path), prior_ownership_roots=prior_roots,
            environment_check=_private_netlist_environment_check(prepared, config),
        )
    jobs = (netlist_optimizer.parallel_candidates if netlist_optimizer is not None else
            min(max(1, int(spec.get("jobs", 1))), int(config["runtime"].get("max_parallel_jobs", 5))))
    persistent_jobs = 1 if persistent_backend else jobs
    requested_persistent_worker_lifetime = str(
        config["runtime"].get("persistent_worker_lifetime", "run")
    ).strip().lower()
    if requested_persistent_worker_lifetime not in ("run", "batch"):
        raise ValueError("runtime.persistent_worker_lifetime must be run or batch")
    persistent_worker_lifetime = "run"
    persistent_startup_limit = persistent_jobs * (
        max(0, int(config["runtime"].get("infrastructure_retries", 1))) + 1
    )
    status["requested_parallel_jobs"] = jobs
    status["effective_parallel_jobs"] = persistent_jobs if persistent_backend else jobs
    status["maestro_candidate_execution"] = copy.deepcopy(execution_plan)
    status["maestro_max_jobs"] = execution_plan["max_jobs"]
    if netlist_optimizer is not None:
        status["parallel_execution_policy"] = "shared_spectre_task_pool"
        status["spectre_task_policy"] = copy.deepcopy(netlist_optimizer.policy)
        status["maestro_max_jobs"] = netlist_optimizer.policy["max_jobs"]
        status["private_netlist_protocol"] = "crescendo.private_netlist_optimizer.v1"
    if persistent_backend:
        status["persistent_worker_lifetime"] = "optimization_run"
        status["persistent_worker_count"] = 1
        status["persistent_session_policy"] = "single_maestro_for_optimization_run"
        status["parallel_execution_policy"] = (
            "native_parallel_candidates_single_maestro" if batch_enabled else "serialized_single_maestro"
        )
        if jobs != 1:
            status["parallel_jobs_override"] = {
                "requested": jobs, "effective": 1,
                "reason": "real EDA uses one run-lifetime Maestro session",
            }
        if requested_persistent_worker_lifetime != "run":
            status["worker_lifetime_override"] = {
                "requested": requested_persistent_worker_lifetime,
                "effective": "run",
                "reason": "scheduled batch restarts are disabled",
            }
    atomic_write_json(status_path, status)
    configured_replacement_limit = int(
        config["runtime"].get("max_infrastructure_replacements", 0)
    )
    if configured_replacement_limit < 0:
        raise ValueError("runtime.max_infrastructure_replacements must be non-negative")
    replacement_limit = (0 if _environment_test_run(spec)
                         else configured_replacement_limit or population)
    scheduler_watchdog_grace = max(
        0.0, float(config["runtime"].get("scheduler_watchdog_grace_seconds", 30)),
    )
    persistent_workers = []
    pending_result = None
    pending_assignment = None
    checkpoint_variable_ids = [row["parameter_id"] for row in variables]

    def publish_result_summary(result):
        try:
            reference = data_store.reference()
        except Exception:
            reference = None
        atomic_write_json(os.path.join(run_root, "result.json"), compact_result_summary(result, reference))
    identity_guard_slots = _PersistentIdentityGuardSlots()
    session_health_guards = identity_guard_slots.boundary
    active_session_health_guards = identity_guard_slots.active
    runtime_config = config.get("runtime") or {}
    try:
        active_health_interval = float(runtime_config.get(
            "persistent_active_health_interval_seconds", 2.0,
        ))
    except (TypeError, ValueError, OverflowError):
        raise ValueError("persistent_active_health_interval_seconds must be a finite number")
    if not math.isfinite(active_health_interval) or active_health_interval < 0.5:
        raise ValueError(
            "persistent_active_health_interval_seconds must be finite and at least 0.5"
        )
    try:
        resource_guard_restart_limit = int(runtime_config.get(
            "persistent_resource_guard_max_restarts", 8,
        ))
    except (TypeError, ValueError, OverflowError):
        raise ValueError("persistent_resource_guard_max_restarts must be a positive integer")
    if persistent_backend and resource_guard_restart_limit < 1:
        raise ValueError("persistent_resource_guard_max_restarts must be a positive integer")
    resource_guard_restart_limit = max(1, resource_guard_restart_limit)
    try:
        configured_identity_startup_timeout = float(runtime_config.get(
            "persistent_identity_startup_timeout_seconds",
            runtime_config.get("ocean_startup_timeout_seconds", 90),
        ))
        ocean_startup_timeout = float(runtime_config.get(
            "ocean_startup_timeout_seconds", 90,
        ))
    except (TypeError, ValueError, OverflowError):
        raise ValueError(
            "persistent_identity_startup_timeout_seconds must be a finite positive number"
        )
    if (
            not math.isfinite(configured_identity_startup_timeout)
            or configured_identity_startup_timeout <= 0
            or not math.isfinite(ocean_startup_timeout) or ocean_startup_timeout <= 0):
        raise ValueError(
            "persistent_identity_startup_timeout_seconds must be a finite positive number"
        )
    identity_startup_timeout = max(
        configured_identity_startup_timeout, ocean_startup_timeout,
    )
    try:
        workspace_prepare_timeout = float(runtime_config.get(
            "persistent_workspace_prepare_timeout_seconds", 1800,
        ))
        startup_total_timeout = float(runtime_config.get(
            "persistent_startup_total_timeout_seconds",
            workspace_prepare_timeout + identity_startup_timeout,
        ))
    except (TypeError, ValueError, OverflowError):
        raise ValueError("persistent startup phase timeouts must be finite positive numbers")
    if (
            not math.isfinite(workspace_prepare_timeout) or workspace_prepare_timeout <= 0
            or not math.isfinite(startup_total_timeout) or startup_total_timeout <= 0):
        raise ValueError("persistent startup phase timeouts must be finite positive numbers")

    def new_session_health_guard():
        try:
            return PersistentSessionHealthGuard(
                warmup_samples=runtime_config.get("persistent_health_warmup_samples", 5),
                baseline_samples=runtime_config.get("persistent_health_baseline_samples", 3),
                consecutive_limit=runtime_config.get("persistent_health_consecutive_limit", 3),
                rss_growth_limit_kib=int(float(
                    runtime_config.get("persistent_rss_growth_limit_mib", 512)
                ) * 1024),
                fd_growth_limit=runtime_config.get("persistent_fd_growth_limit", 128),
                min_free_disk_mib=runtime_config.get("persistent_min_free_disk_mib", 1024),
                log_growth_limit_bytes=int(float(
                    runtime_config.get("persistent_log_growth_limit_mib", 256)
                ) * 1024 * 1024),
            )
        except (TypeError, ValueError, OverflowError):
            raise ValueError("persistent Maestro health limits must be finite positive numbers")

    def controller_health_identity(worker, health):
        process = getattr(worker, "process", None)
        expected = (
            getattr(worker, "epoch_index", None),
            getattr(process, "pid", None),
            getattr(worker, "_start_ticks", None),
        )
        observed = (
            health.get("controller_epoch"), health.get("pid"),
            health.get("pid_start_ticks"),
        )
        if (
                any(isinstance(value, bool) or value is None for value in expected)
                or observed != expected):
            return None
        return expected

    def session_health_identity(worker, health):
        controller_identity = controller_health_identity(worker, health)
        session = health.get("session_identity")
        if (
                controller_identity is None
                or health.get("session_identity_proven") is not True
                or not isinstance(session, dict) or session.get("proven") is not True):
            return None
        expected = {
            "worker_index": worker.worker_index,
            "controller_epoch": controller_identity[0],
        }
        observed = {
            "worker_index": session.get("worker_index"),
            "controller_epoch": session.get("controller_epoch"),
        }
        epoch = session.get("epoch")
        session_pid = session.get("pid")
        session_start_ticks = session.get("pid_start_ticks")
        if (
                observed != expected or not isinstance(epoch, str) or not epoch
                or isinstance(session_pid, bool) or not isinstance(session_pid, int)
                or session_pid <= 1 or isinstance(session_start_ticks, bool)
                or not isinstance(session_start_ticks, int) or session_start_ticks <= 0):
            return None
        return {
            "key": controller_identity + (epoch, session_pid, session_start_ticks),
            "controller_key": controller_identity,
            "controller_epoch": controller_identity[0],
            "worker_epoch": epoch,
            "controller_pid": controller_identity[1],
            "controller_pid_start_ticks": controller_identity[2],
            "session_pid": session_pid,
            "session_pid_start_ticks": session_start_ticks,
        }

    def unproven_identity_health(health):
        observed = dict(health)
        observed.update({
            "session_identity_complete": False,
            "rss_kib": None, "rss_kb": None, "fd_count": None,
            "max_fd_count": None, "log_bytes": None,
        })
        return observed

    def pending_identity_health(health):
        observed = dict(health)
        observed.update({
            "session_identity_complete": True,
            "rss_kib": None, "rss_kb": None, "fd_count": None,
            "max_fd_count": None,
        })
        return observed

    def public_identity_failure_reason(health):
        session = health.get("session_identity") if isinstance(health, dict) else None
        raw = str((session or {}).get("reason") or "").lower()
        reasons = (
            ("outside the active controller namespace", "foreign_worker_epoch",
             "worker epochs outside the active controller namespace exist"),
            ("expected exactly one active worker epoch", "worker_epoch_not_materialized",
             "exactly one active worker epoch is not yet available"),
            ("state identity mismatch", "worker_state_identity_mismatch",
             "active worker state identity does not match its controller namespace"),
            ("session is not reusable", "worker_session_not_reusable",
             "active worker session is not in a reusable state"),
            ("process identity is incomplete", "worker_process_identity_incomplete",
             "active worker process identity is incomplete"),
            ("not in the owned controller tree", "worker_process_not_owned",
             "active worker process is not in the owned controller tree"),
            ("authenticated worker health did not match", "worker_health_identity_mismatch",
             "authenticated worker health does not match the active session"),
            ("no such file or directory", "worker_state_not_materialized",
             "active worker identity files are not yet available"),
        )
        for fragment, code, message in reasons:
            if fragment in raw:
                return {"code": code, "message": message}
        return {
            "code": "session_identity_unproven",
            "message": "active worker session identity could not be proven",
        }

    def new_identity_guard_entry(identity, controller_identity, observed_at,
                                 authorized=False):
        return {
            "identity": identity,
            "controller_identity": controller_identity,
            "guard": new_session_health_guard(),
            "authorized": bool(authorized),
            "identity_ever_proven": identity is not None,
            "identity_pending_since": None,
            "initial_startup": identity is None,
        }

    def expect_session_replacement(worker):
        process = getattr(worker, "process", None)
        controller_identity = (
            getattr(worker, "epoch_index", None),
            getattr(process, "pid", None),
            getattr(worker, "_start_ticks", None),
        )
        if any(isinstance(value, bool) or value is None for value in controller_identity):
            return False
        identity_guard_slots.expect_replacement(
            worker.worker_index, controller_identity, new_identity_guard_entry,
        )
        return True

    def begin_expected_session_replacement(worker, observed_at):
        return identity_guard_slots.begin_expected_replacement(
            worker.worker_index, observed_at,
        )

    def observe_identity_pending(guard_entry, health, observed_at,
                                 start_deadline=True):
        decision = guard_entry["guard"].observe(pending_identity_health(health))
        started_at = guard_entry.get("identity_pending_since")
        if started_at is None:
            if not start_deadline and guard_entry.get("expected_replacement") is True:
                decision.update({
                    "state": "replacement_waiting_for_evaluate",
                    "triggered": False,
                    "identity_pending_seconds": 0.0,
                    "identity_startup_timeout_seconds": identity_startup_timeout,
                    "identity_unproven_reason": public_identity_failure_reason(health),
                })
                return decision
            guard_entry["identity_pending_since"] = observed_at
            started_at = observed_at
        elapsed = max(0.0, observed_at - started_at)
        phase = health.get("startup_phase")
        phase_proven = (
            isinstance(phase, dict) and phase.get("proven") is True
            and phase.get("controller_epoch") == health.get("controller_epoch")
            and phase.get("controller_pid") == health.get("pid")
            and phase.get("controller_pid_start_ticks") == health.get("pid_start_ticks")
        )
        timeout_state = None
        timeout_violation = None
        public_phase = "identity_starting"
        phase_limit = identity_startup_timeout
        total_elapsed = elapsed
        if phase_proven:
            phase_key = (
                phase.get("controller_epoch"), phase.get("controller_pid"),
                phase.get("controller_pid_start_ticks"),
                phase.get("request_sequence"), phase.get("request_nonce"),
            )
            previous_key = guard_entry.get("startup_request_identity")
            if previous_key is not None and previous_key != phase_key:
                timeout_state = "startup_request_identity_changed"
                timeout_violation = "startup_request_identity_changed"
            else:
                guard_entry["startup_request_identity"] = phase_key
                total_started = min(
                    started_at, phase["request_started_monotonic"],
                    phase["started_monotonic"],
                )
                previous_total = guard_entry.get("startup_total_started")
                if previous_total is not None:
                    total_started = min(total_started, previous_total)
                guard_entry["startup_total_started"] = total_started
                total_elapsed = max(0.0, observed_at - total_started)
                first_ocean = guard_entry.get("startup_ocean_started")
                phase_ocean = phase.get("first_ocean_started_monotonic")
                if phase_ocean is not None:
                    first_ocean = min(first_ocean, phase_ocean) if first_ocean is not None else phase_ocean
                    guard_entry["startup_ocean_started"] = first_ocean
                public_phase = phase.get("stage")
                if first_ocean is None and public_phase == "workspace_prepare":
                    elapsed = total_elapsed
                    phase_limit = workspace_prepare_timeout
                    if elapsed >= phase_limit:
                        timeout_state = "workspace_prepare_timeout"
                        timeout_violation = "workspace_prepare_timeout"
                else:
                    # A repeated preparation marker cannot reset the first Ocean deadline.
                    first_ocean = first_ocean if first_ocean is not None else total_started
                    elapsed = max(0.0, observed_at - first_ocean)
                    if elapsed >= identity_startup_timeout:
                        timeout_state = "identity_startup_timeout"
                        timeout_violation = "session_identity_startup_timeout"
                if total_elapsed >= startup_total_timeout:
                    timeout_state = "startup_total_timeout"
                    timeout_violation = "persistent_startup_total_timeout"
        elif elapsed >= identity_startup_timeout:
            timeout_state = "identity_startup_timeout"
            timeout_violation = "session_identity_startup_timeout"
        if timeout_state is not None:
            violations = list(decision.get("violations") or [])
            if timeout_violation not in violations:
                violations.append(timeout_violation)
            decision.update({
                "state": timeout_state,
                "triggered": True,
                "violations": violations,
            })
        elif not decision.get("triggered"):
            decision.update({
                "state": "workspace_preparing" if public_phase == "workspace_prepare" else "identity_pending",
                "triggered": False,
            })
        decision.update({
            "identity_pending_seconds": elapsed,
            "identity_startup_timeout_seconds": identity_startup_timeout,
            "identity_unproven_reason": public_identity_failure_reason(health),
            "startup_phase": public_phase,
            "startup_phase_authenticated": phase_proven,
            "startup_phase_timeout_seconds": phase_limit,
            "startup_total_seconds": total_elapsed,
            "startup_total_timeout_seconds": startup_total_timeout,
        })
        return decision

    def observe_unexpected_identity_change(guard_entry, health, identity, violation):
        decision = guard_entry["guard"].observe(unproven_identity_health(health))
        violations = list(decision.get("violations") or [])
        if violation not in violations:
            violations.append(violation)
        decision.update({
            "state": violation,
            "triggered": True,
            "violations": violations,
            "identity_unproven_reason": {
                "code": violation,
                "message": "session identity changed without an authorized replacement",
            },
            "previous_session_identity": public_session_identity(
                guard_entry.get("identity")
            ),
            "observed_session_identity": public_session_identity(identity),
        })
        return decision

    def public_cleanup_result(cleanup_result):
        public = copy.deepcopy(cleanup_result)
        health = public.get("health") if isinstance(public, dict) else None
        session = health.get("session_identity") if isinstance(health, dict) else None
        if isinstance(session, dict) and session.get("reason"):
            reason = public_identity_failure_reason(health)
            session["reason"] = reason["message"]
            session["reason_code"] = reason["code"]
        return public

    def public_session_identity(identity):
        if identity is None:
            return None
        return dict((key, value) for key, value in identity.items() if key != "key")

    def observe_session_health(worker, cleanup_result):
        if runtime_config.get("persistent_health_enabled", True) is False:
            return {
                "schema_version": "crescendo.persistent_session_health.v1",
                "state": "disabled", "triggered": False,
            }
        health = (cleanup_result or {}).get("health")
        if not isinstance(health, dict):
            return None
        observed_at = time.monotonic()
        controller_identity = controller_health_identity(worker, health)
        identity = session_health_identity(worker, health)
        guard_entry = session_health_guards.get(worker.worker_index)
        transition = "same_session"
        expected_replacement_transition = False
        forced_decision = None
        if guard_entry is None:
            guard_entry = new_identity_guard_entry(
                identity, controller_identity, observed_at,
            )
            session_health_guards[worker.worker_index] = guard_entry
            transition = "initial_session" if identity is not None else "initial_startup"
        elif (
                controller_identity is not None
                and guard_entry.get("controller_identity") != controller_identity):
            transition = "unexpected_controller_identity_change"
            forced_decision = observe_unexpected_identity_change(
                guard_entry, health, identity, transition,
            )
        elif identity is not None and guard_entry.get("identity") is None:
            expected_replacement_transition = bool(guard_entry.get("expected_replacement"))
            if expected_replacement_transition or guard_entry.get("initial_startup") is True:
                transition = (
                    "expected_replacement_proven"
                    if expected_replacement_transition else "initial_session"
                )
                guard_entry = new_identity_guard_entry(
                    identity, controller_identity, observed_at,
                )
                session_health_guards[worker.worker_index] = guard_entry
            else:
                transition = "unexpected_session_identity_change"
                forced_decision = observe_unexpected_identity_change(
                    guard_entry, health, identity, transition,
                )
        elif identity is not None and (
                guard_entry["identity"].get("key") != identity.get("key")):
            transition = "unexpected_session_identity_change"
            forced_decision = observe_unexpected_identity_change(
                guard_entry, health, identity, transition,
            )
        if forced_decision is not None:
            decision = forced_decision
        elif identity is None and not guard_entry.get("identity_ever_proven"):
            decision = observe_identity_pending(
                guard_entry, health, observed_at, start_deadline=False,
            )
        elif identity is None:
            transition = "identity_unproven"
            decision = guard_entry["guard"].observe(unproven_identity_health(health))
            decision["identity_unproven_reason"] = public_identity_failure_reason(health)
        else:
            decision = guard_entry["guard"].observe(health)
        decision["controller_epoch"] = getattr(worker, "epoch_index", None)
        decision["worker_index"] = worker.worker_index
        decision["identity_proven"] = identity is not None
        decision["identity_transition"] = transition
        decision["session_identity"] = public_session_identity(identity)
        decision["expected_replacement"] = bool(
            guard_entry.get("expected_replacement") or expected_replacement_transition
        )
        return decision

    def authorize_active_session_health(worker, cleanup_result, boundary_decision):
        if runtime_config.get("persistent_health_enabled", True) is False:
            return
        health = (cleanup_result or {}).get("health")
        if not isinstance(health, dict):
            return
        if not (
            health.get("alive") is True and health.get("tree_complete") is not False
            and health.get("storage_complete") is not False
            and health.get("logs_complete") is not False
        ):
            return
        identity = session_health_identity(worker, health)
        if (
                identity is None or not isinstance(boundary_decision, dict)
                or boundary_decision.get("identity_proven") is not True
                or (boundary_decision.get("session_identity") or {}).get("worker_epoch")
                != identity.get("worker_epoch")):
            return
        guard_entry = active_session_health_guards.get(worker.worker_index)
        if (
                guard_entry is not None and guard_entry.get("authorized") is True
                and guard_entry.get("identity") is not None
                and guard_entry["identity"].get("key") == identity.get("key")):
            return
        guard = new_session_health_guard()
        seed_decision = guard.observe(health, comparable=False)
        guard_entry = new_identity_guard_entry(
            identity, identity.get("controller_key"), time.monotonic(), authorized=True,
        )
        guard_entry.update({"guard": guard, "seed_decision": seed_decision})
        active_session_health_guards[worker.worker_index] = guard_entry

    def observe_active_session_health(worker, health):
        if runtime_config.get("persistent_health_enabled", True) is False:
            return None
        controller_identity = controller_health_identity(worker, health)
        identity = session_health_identity(worker, health)
        observed_at = time.monotonic()
        guard_entry = active_session_health_guards.get(worker.worker_index)
        transition = "same_session"
        identity_unproven = False
        expected_replacement_transition = False
        forced_decision = None
        if guard_entry is None:
            guard_entry = new_identity_guard_entry(
                identity, controller_identity, observed_at,
            )
            active_session_health_guards[worker.worker_index] = guard_entry
            transition = "initial_session" if identity is not None else "initial_startup"
        elif (
                controller_identity is not None
                and guard_entry.get("controller_identity") != controller_identity):
            transition = "unexpected_controller_identity_change"
            forced_decision = observe_unexpected_identity_change(
                guard_entry, health, identity, transition,
            )
        elif identity is not None and (
                guard_entry.get("identity") is None
                or guard_entry["identity"].get("key") != identity.get("key")):
            prior_identity = guard_entry.get("identity")
            expected_replacement = guard_entry.get("expected_replacement") is True
            expected_replacement_transition = expected_replacement
            if expected_replacement or (
                    prior_identity is None and guard_entry.get("initial_startup") is True):
                guard_entry = new_identity_guard_entry(
                    identity, controller_identity, observed_at,
                )
                active_session_health_guards[worker.worker_index] = guard_entry
                transition = (
                    "expected_replacement_proven" if expected_replacement
                    else "initial_session_proven"
                )
            else:
                transition = "unexpected_session_identity_change"
                forced_decision = observe_unexpected_identity_change(
                    guard_entry, health, identity, transition,
                )
        elif identity is None and (
                guard_entry.get("identity") is not None
                or guard_entry.get("authorized") is True):
            transition = "identity_unproven"
            identity_unproven = True
        if forced_decision is not None:
            decision = forced_decision
        elif identity is None and not guard_entry.get("identity_ever_proven"):
            decision = observe_identity_pending(guard_entry, health, observed_at)
        else:
            observed_health = health
            if identity_unproven:
                observed_health = unproven_identity_health(health)
            elif not guard_entry["authorized"]:
                observed_health = pending_identity_health(health)
            decision = guard_entry["guard"].observe(
                observed_health,
                comparable=bool(identity_unproven or not guard_entry["authorized"]),
            )
            if identity_unproven:
                decision["identity_unproven_reason"] = public_identity_failure_reason(health)
        decision.update({
            "controller_epoch": (
                controller_identity[0] if controller_identity is not None else None
            ),
            "worker_index": worker.worker_index,
            "phase": "active_evaluate",
            "resource_limits_authorized": bool(guard_entry["authorized"]),
            "identity_proven": identity is not None,
            "identity_transition": transition,
            "session_identity": public_session_identity(identity),
            "expected_replacement": bool(
                guard_entry.get("expected_replacement") or expected_replacement_transition
            ),
        })
        if health.get("sampling_error"):
            decision["sampling_error"] = health["sampling_error"]
        return decision

    anchor_root = checkpoint_anchor_root or os.path.dirname(run_root)

    def persist_checkpoint(path, document, description):
        if os.path.lexists(path) and (os.path.islink(path) or not os.path.isfile(path)):
            raise RuntimeError("%s is not a safe regular file" % description)
        validate_checkpoint_integrity(document)
        atomic_write_text(
            path, json.dumps(document, separators=(",", ":"), sort_keys=True) + "\n",
        )
        if os.path.islink(path) or not os.path.isfile(path):
            raise RuntimeError("%s was not persisted safely" % description)
        persisted = load_json(path)
        validate_checkpoint_integrity(persisted)
        if persisted.get("payload_sha256") != document.get("payload_sha256"):
            raise RuntimeError("%s verification failed" % description)

    def begin_resource_guard_recycle(generation, plan, worker, candidate_id,
                                     phase, decision):
        restart_count = int(status.get("resource_guard_restart_count", 0))
        exhausted = restart_count >= resource_guard_restart_limit
        if not exhausted:
            restart_count += 1
            status["resource_guard_restart_count"] = restart_count
        event = {
            "candidate_id": candidate_id,
            "worker_index": worker.worker_index,
            "phase": phase,
            "decision": decision,
            "requested_at": utc_timestamp(),
            "restart_number": restart_count,
            "restart_limit": resource_guard_restart_limit,
            "restart_budget_exhausted": exhausted,
            "state": "restart_limit_exhausted" if exhausted else "checkpointed",
        }
        status.setdefault("resource_guard_recycles", []).append(event)
        status["resource_guard_recycles"] = status["resource_guard_recycles"][-20:]
        if phase == "active_candidate":
            status.setdefault("active_resource_guard_terminations", []).append(event)
            status["active_resource_guard_terminations"] = (
                status["active_resource_guard_terminations"][-20:]
            )
        status["persistent_session_health"] = decision
        status["message"] = (
            "persistent Maestro resource guard restart limit exhausted; "
            "checkpoint saved without failing the candidate"
            if exhausted else
            "persistent Maestro resource guard exceeded; checkpoint saved before clean restart"
        )
        atomic_write_json(status_path, status)
        save_checkpoint(generation, plan)
        return event, exhausted

    def resource_guard_is_recyclable(decision, phase):
        violations = set((decision or {}).get("violations") or [])
        recyclable = set((
            "rss_growth", "fd_growth", "fd_per_process_limit", "log_growth",
        ))
        if not violations or not violations.issubset(recyclable):
            return False
        if (decision or {}).get("identity_proven") is not True:
            return False
        if phase == "active_candidate" and (
                decision or {}).get("resource_limits_authorized") is not True:
            return False
        return True

    def terminate_resource_guard_epoch(worker, event):
        termination = worker.terminate_and_prove(
            "persistent Maestro resource guard recycle for %s" % event["candidate_id"]
        )
        event["termination"] = termination
        event["terminated_at"] = utc_timestamp()
        if not (
                termination.get("termination_proven") is True
                and termination.get("process_still_alive") is False
                and (termination.get("cleanup") or {}).get("ok") is True):
            event["state"] = "termination_unproven"
            atomic_write_json(status_path, status)
            raise RuntimeError(
                "resource guard could not prove owned controller cleanup: %s" % termination
            )
        source_guard = (
            _reverify_prepared_source_identity(prepared, config)
            if prepared.get("source_identity") is not None else
            {"ok": True, "state": "source_identity_not_recorded"}
        )
        event["source_guard"] = source_guard
        if not source_guard.get("ok"):
            event["state"] = "source_integrity_failed"
            atomic_write_json(status_path, status)
            raise RuntimeError(
                source_guard.get("error") or
                "source identity changed during resource guard recycle"
            )
        event["state"] = (
            "terminated_restart_limit_exhausted"
            if event.get("restart_budget_exhausted") else "terminated"
        )
        atomic_write_json(status_path, status)
        return termination

    def restart_resource_guard_epoch(generation, plan, worker, event):
        if os.path.exists(stop_path):
            event["state"] = "stopped_after_termination"
            status["message"] = "user_stop"
            atomic_write_json(status_path, status)
            save_checkpoint(generation, plan)
            return False
        worker.restart()
        if not expect_session_replacement(worker):
            event["state"] = "replacement_identity_unproven"
            atomic_write_json(status_path, status)
            raise RuntimeError(
                "resource guard replacement controller identity could not be established"
            )
        event["state"] = "restarted"
        event["restarted_at"] = utc_timestamp()
        event["controller_epoch"] = getattr(worker, "epoch_index", None)
        status["message"] = (
            "persistent Maestro restarted after resource guard; continuing optimization"
        )
        atomic_write_json(status_path, status)
        save_checkpoint(generation, plan)
        return True

    def save_checkpoint(next_generation, plan, terminal_state=None, terminal_reason=None):
        nonlocal checkpoint_ancestry, checkpoint_parent_checksum, terminal_checkpoint_written
        status["checkpoint_revision"] = int(status.get("checkpoint_revision", 0)) + 1
        ancestry = list(checkpoint_ancestry)
        if checkpoint_parent_checksum and (
            not ancestry or ancestry[-1] != checkpoint_parent_checksum
        ):
            ancestry.append(checkpoint_parent_checksum)
        if plan is not None:
            planned_rows = []
            for index, values in plan.get("planned") or []:
                identity = "g%03d_c%04d" % (plan["generation"], index)
                if identity not in planned_core_ids:
                    planned_rows.append({"candidate_id": identity, "generation": plan["generation"],
                                         "index": index, "values": values, "status": "planned",
                                         "execution_backend": spec["execution_backend"]})
                    planned_core_ids.add(identity)
            if planned_rows:
                data_store.commit_candidates(planned_rows)
        document = _checkpoint_document(
            spec, prepared, generator, all_rows, selection_history, next_generation, plan, status,
            ancestry=ancestry, terminal_state=terminal_state,
            terminal_reason=terminal_reason,
            candidate_layout=(resume_checkpoint or {}).get("candidate_layout"),
            candidate_store=data_store.reference(),
        )
        startup_storage_preflight(
            run_root, anchor_root,
            runtime_config.get("persistent_min_free_disk_mib", 1024),
            checkpoint=document, workspace_required=False,
        )
        anchor_path = continuation_checkpoint_path(
            anchor_root, continuation_id, create=True,
        )
        anchored_checksum = None
        if os.path.lexists(anchor_path):
            if os.path.islink(anchor_path) or not os.path.isfile(anchor_path):
                raise RuntimeError("continuation checkpoint anchor is not a safe regular file")
            anchored = load_json(anchor_path)
            validate_checkpoint_integrity(anchored)
            if (
                checkpoint_continuation_id(anchored) != continuation_id
                or checkpoint_continuation_schema(anchored)
                != checkpoint_continuation_schema(document)
            ):
                raise RuntimeError(
                    "continuation checkpoint anchor belongs to a different optimization"
                )
            anchored_checksum = _checkpoint_checksum(anchored)
            if checkpoint_parent_checksum is None:
                if anchored_checksum != document.get("payload_sha256"):
                    raise RuntimeError("continuation checkpoint anchor appeared unexpectedly")
            elif anchored_checksum != checkpoint_parent_checksum:
                raise RuntimeError(
                    "continuation checkpoint anchor changed outside the held lease"
                )
        pending_path = continuation_pending_path(
            anchor_root, continuation_id, create=True,
        )
        if os.path.lexists(pending_path):
            raise RuntimeError(
                "an unresolved continuation pending journal must be recovered before optimization"
            )
        pending = build_continuation_pending(
            document, continuation_id,
            int((document.get("state") or {}).get("checkpoint_revision", 0)),
            anchored_checksum,
            continuation_schema_sha256(checkpoint_continuation_schema(document)),
        )
        persist_continuation_pending(anchor_root, continuation_id, pending)
        persist_checkpoint(
            os.path.join(run_root, "checkpoint.json"), document, "run checkpoint",
        )
        persist_checkpoint(anchor_path, document, "continuation checkpoint anchor")
        clear_continuation_pending(anchor_root, continuation_id)
        checkpoint_ancestry = ancestry
        checkpoint_parent_checksum = document["payload_sha256"]
        if terminal_state == "completed":
            terminal_checkpoint_written = True

    live_candidate_rows = {}
    live_batch_snapshots = {}

    def update_live_batch(planned, generation, initializing=False, serial=False):
        from .maestro_progress import read_batch_progress
        ids = ['g%03d_c%04d' % (generation, index) for index, _values in planned]
        path = (os.path.join(run_root, 'candidates', ids[0], 'execution_progress.json') if serial else
                os.path.join(run_root, 'batches', 'g%03d_%s' % (generation, ids[0]), 'execution_progress.json'))
        snapshot = read_batch_progress(path, run_root, ids)
        key = json.dumps(snapshot, sort_keys=True) if snapshot else None
        if not initializing and (snapshot is None or live_batch_snapshots.get(path) == key):
            return
        supplied = dict((row['candidate_id'], row.get('execution_progress') or {})
                        for row in (snapshot or {}).get('candidates') or [])
        rows = []
        for index, values in planned:
            cid = 'g%03d_c%04d' % (generation, index)
            progress = copy.deepcopy(supplied.get(cid) or {'stage': 'preparing', 'tasks': []})
            previous = live_candidate_rows.get(cid)
            row = {'candidate_id': cid, 'generation': generation, 'index': index,
                   'values': copy.deepcopy(values), 'status': 'running' if progress.get('stage') in
                   ('running', 'evaluating') else 'queued', 'execution_backend': 'persistent_maestro',
                   'execution_progress': progress, 'source_test_map': progress.get('source_test_map') or {},
                   'spectre_state': 'started' if progress.get('stage') in ('running', 'evaluating') or any(task.get('state') in ('running', 'finished') for task in progress.get('tasks') or []) else 'not_started'}
            if previous is not None and previous == row:
                continue
            live_candidate_rows[cid] = row
            rows.append(row)
            append_run_event(run_root, 'candidate', progress.get('stage') or row['status'],
                {'candidate_id': cid, 'execution_backend': 'persistent_maestro',
                 'status': row['status'], 'execution_progress': progress})
        if rows:
            data_store.commit_candidates(rows)
        if snapshot:
            live_batch_snapshots[path] = key
            status['maestro_live_progress'] = {'batch_id': snapshot['batch_id'],
                'observed_at': snapshot.get('observed_at'), 'progress_path': path,
                'shared_submissions': snapshot.get('shared_submissions') or [],
                'candidates': dict((row['candidate_id'], (row.get('execution_progress') or {}).get('stage'))
                                   for row in snapshot['candidates'])}
            atomic_write_json(status_path, status)

    def request_point_commit(point, cancel_check):
        request = {"point": point, "ready": threading.Event(), "result": None, "error": None}
        point_commit_queue.put(request)
        deadline = time.monotonic() + max(30.0, float(runtime_config.get("point_commit_timeout_seconds", 120)))
        while not request["ready"].wait(0.1):
            if cancel_check() or time.monotonic() >= deadline:
                return {"ok": False, "state": "cancelled_or_timeout_before_point_cleanup"}
        if request["error"] is not None:
            raise request["error"]
        return request["result"]

    def drain_point_commits(generation):
        requests = []
        while True:
            try:
                requests.append(point_commit_queue.get_nowait())
            except queue.Empty:
                break
        if not requests:
            return
        try:
            for request in requests:
                request["revision"] = data_store.append_event("point", request["point"])
            # One checkpoint ACK covers all points currently waiting. Candidate
            # status and the plan/RNG stay unchanged; a partial point is never
            # reported completed or skipped on a fresh Resume.
            save_checkpoint(generation, active_plan)
            for request in requests:
                request["result"] = {"ok": True, "revision": request["revision"],
                                     "checkpoint_sha256": checkpoint_parent_checksum,
                                     "candidate_completed": False}
        except Exception as error:
            for request in requests:
                request["error"] = error
            raise
        finally:
            for request in requests:
                request["ready"].set()

    if netlist_optimizer is not None:
        netlist_optimizer.point_commit_callback = request_point_commit

    def fold_successful_candidate(row, native_cleanup_proven):
        if row.get("status") != "completed" or not native_cleanup_proven:
            return None
        candidate_root = os.path.join(run_root, "candidates", row["candidate_id"])
        if not os.path.isdir(candidate_root):
            return None
        for key, name in (("timing", "timing.json"), ("completion_evidence", "completion_evidence.json")):
            path = os.path.join(candidate_root, name)
            if os.path.isfile(path) and not os.path.islink(path):
                row[key] = load_json(path)
        summary = summarize_successful_candidate_artifacts(candidate_root, run_root)
        if not summary:
            return None
        row["artifact_summary"] = summary
        row["raw_cleanup_proof"] = {"ok": True, "process_termination_proven": True,
                                    "remote_stop_proven": True, "native_cleanup_proven": native_cleanup_proven}
        data_store.commit_candidates([row])
        save_checkpoint(generation, active_plan)
        try:
            result = compact_successful_candidate_artifacts(candidate_root, run_root)
        except Exception as error:
            # A stopped task's small summary deletion is housekeeping. The
            # durable core/checkpoint remain valid and the exact files stay
            # available; an artifact error must not impersonate native failure.
            result = {"ok": False, "artifact_cleanup_complete": False,
                      "candidate_id": row["candidate_id"], "error": "%s: %s" % (error.__class__.__name__, error)}
        row["artifact_compaction"] = result
        data_store.commit_candidates([row])
        return result

    try:
        save_checkpoint(
            start_generation, active_plan, terminal_state=resume_terminal_state,
            terminal_reason=resume_terminal_reason,
        )
        if (
            persistent_backend and persistent_worker_lifetime == "run"
            and execution_start_generation < generations
        ):
            for worker_index in range(persistent_jobs):
                controller = PersistentControllerProxy(
                    run_root, worker_index, prepared, selected, config,
                )
                controller.start()
                persistent_workers.append(controller)
        for generation in range(execution_start_generation, generations):
            if os.path.exists(stop_path):
                stopped_reason = "user_stop"
                break
            status.update({"generation": generation + 1, "message": "evaluating generation %d" % (generation + 1)})
            atomic_write_json(status_path, status)
            generation_rows = [
                row for row in all_rows
                if row.get("generation") == generation and row.get("status") != "infrastructure_failed"
            ]
            infrastructure_rows = [
                row for row in all_rows
                if row.get("generation") == generation and row.get("status") == "infrastructure_failed"
            ]
            persistent_startup_streak = 0
            _raise_workspace_build_failure(
                generation, generation_rows + infrastructure_rows,
            )
            _raise_source_integrity_failure(generation, infrastructure_rows)
            if active_plan is None or active_plan.get("generation") != generation:
                initial_values = generator.ask(population)
                active_plan = {
                    "generation": generation,
                    "next_index": len(initial_values),
                    "replacements": 0,
                    "planned": list(enumerate(initial_values)),
                }
                save_checkpoint(generation, active_plan)

            def record_future_rows(future_rows, cleanup_worker, startup_streak):
                if not isinstance(future_rows, list):
                    future_rows = [future_rows]
                for row in future_rows:
                    if spec.get("evaluator", "real") == "real":
                        row.setdefault("execution_backend", spec["execution_backend"])
                        row["execution_simulator"] = spec["simulator"]
                    live_row = live_candidate_rows.get(row['candidate_id']) or {}
                    for live_key in ('execution_progress', 'source_test_map'):
                        if live_row.get(live_key):
                            row[live_key] = copy.deepcopy(live_row[live_key])
                    if row.get('execution_progress'):
                        progress = row['execution_progress']
                        progress['stage'] = row.get('status')
                        points = (row.get('completion_evidence') or {}).get('points') or []
                        for task in progress.get('tasks') or []:
                            matching = [point for point in points if
                                point.get('artifact_test') == task.get('artifact_test') and
                                str(point.get('point_id')) == str(task.get('point_id'))]
                            if len(matching) == 1:
                                task['corner'] = matching[0].get('source_corner') or matching[0].get('corner')
                    evaluated_row = row
                    candidate_root = os.path.join(run_root, "candidates", row["candidate_id"])
                    candidate_path = os.path.join(candidate_root, "candidate.json")
                    cleanup_request = None
                    process_cleanup_unproven = bool(evaluated_row.get("cleanup_pending"))
                    candidate_is_persisted = (
                        spec.get("evaluator", "real") == "real" and os.path.isfile(candidate_path)
                    )
                    if candidate_is_persisted:
                        persist_lightweight_candidate(candidate_root, row)
                        required_paths = [
                            candidate_path,
                            os.path.join(candidate_root, "timing.json"),
                        ]
                        if row.get("status") == "completed":
                            required_paths.append(os.path.join(candidate_root, "completion_evidence.json"))
                        if row.get("execution_backend") == "private_netlist":
                            diagnostic_path = os.path.join(candidate_root, "execution_diagnostics.json")
                            if os.path.isfile(diagnostic_path):
                                required_paths.append(diagnostic_path)
                        if row.get("source_guard", {}).get("ok") and not process_cleanup_unproven:
                            cleanup_request = (candidate_root, row["source_guard"], required_paths)
                        row = lightweight_candidate_record(row)
                        for key, name in (("timing", "timing.json"), ("completion_evidence", "completion_evidence.json")):
                            path = os.path.join(candidate_root, name)
                            if os.path.isfile(path) and not os.path.islink(path):
                                row[key] = load_json(path)
                    data_store.commit_candidates([row])
                    all_rows.append(row)
                    if row["status"] == "infrastructure_failed":
                        infrastructure_rows.append(row)
                    else:
                        generation_rows.append(row)
                    status["completed_candidates"] = len([
                        item for item in all_rows if item["status"] == "completed"
                    ])
                    status["failed_candidates"] = len([
                        item for item in all_rows if item["status"] == "failed"
                    ])
                    status["infrastructure_failed_candidates"] = len([
                        item for item in all_rows if item["status"] == "infrastructure_failed"
                    ])
                    status["message"] = "%s %s" % (row["candidate_id"], row["status"])
                    atomic_write_json(status_path, status)

                    # The terminal candidate must be durable before the shared Maestro
                    # session discards its only raw result/history binding.
                    save_checkpoint(generation, active_plan)
                    compact_before_cleanup = json.dumps({
                        "candidate": _pack_candidate(row, checkpoint_variable_ids),
                        "counts": [
                            status.get("completed_candidates", 0),
                            status.get("failed_candidates", 0),
                            status.get("infrastructure_failed_candidates", 0),
                        ],
                    }, separators=(",", ":"), sort_keys=True)
                    cleanup_changed = False
                    health_limit_decision = None
                    fatal_cleanup_error = (
                        "owned simulation cleanup was not proven; resume must quiesce the "
                        "recorded tasks before continuing"
                        if process_cleanup_unproven else None
                    )
                    if cleanup_worker is not None:
                        persistent_state = row.setdefault("persistent_worker", {})
                        try:
                            cleanup_result = cleanup_worker.commit_candidate(row["candidate_id"])
                            if not isinstance(cleanup_result, dict):
                                raise RuntimeError("persistent worker returned an invalid cleanup result")
                        except Exception as error:
                            cleanup_result = {
                                "ok": False, "state": "cleanup_exception",
                                "error": "%s: %s" % (error.__class__.__name__, error),
                            }
                            persistent_state["cleanup"] = public_cleanup_result(cleanup_result)
                            persistent_state["cleanup_pending"] = True
                            cleanup_request = None
                            cleanup_changed = True
                            fatal_cleanup_error = "persistent worker candidate cleanup raised an exception"
                        else:
                            retirement = cleanup_result.get("retirement") or {}
                            retired_guard = retirement.get("source_guard") or {}
                            retirement_cleanup = retirement.get("cleanup") or {}
                            if retirement and not retired_guard:
                                retired_guard = _reverify_prepared_source_identity(
                                    prepared, config,
                                )
                                retirement["source_guard"] = retired_guard
                            safe_retirement = bool(
                                retirement
                                and retirement.get("termination_proven") is True
                                and retirement.get("process_still_alive") is False
                                and retired_guard.get("ok") is True
                                and retirement_cleanup.get("ok") is True
                            )
                            cleanup_complete = cleanup_result.get("ok") is True or safe_retirement
                            persistent_state["cleanup_pending"] = not cleanup_complete
                            if not cleanup_complete:
                                cleanup_request = None
                            expected_replacement = bool(
                                cleanup_result.get("recycled") is True and safe_retirement
                                and expect_session_replacement(cleanup_worker)
                            )
                            if expected_replacement:
                                persistent_state["expected_session_replacement"] = True
                            if cleanup_complete:
                                health_decision = observe_session_health(
                                    cleanup_worker, cleanup_result,
                                )
                                if health_decision is not None:
                                    persistent_state["session_health"] = health_decision
                                    cleanup_changed = True
                                    if health_decision.get("triggered"):
                                        health_limit_decision = health_decision
                                    else:
                                        authorize_active_session_health(
                                            cleanup_worker, cleanup_result,
                                            health_decision,
                                        )
                            if retired_guard and not retired_guard.get("ok"):
                                cleanup_request = None
                                for candidate_row in (row, evaluated_row):
                                    candidate_row["source_guard"] = retired_guard
                                    candidate_row["status"] = "infrastructure_failed"
                                    candidate_row["failure_class"] = "source_integrity"
                                    candidate_row["failure_stage"] = "source_integrity"
                                    candidate_row["error"] = retired_guard.get("error") or (
                                        "source library fingerprint changed or could not be verified"
                                    )
                                    candidate_row.pop("metrics", None)
                                if row in generation_rows:
                                    generation_rows.remove(row)
                                if row not in infrastructure_rows:
                                    infrastructure_rows.append(row)
                                status["completed_candidates"] = len([
                                    item for item in all_rows if item["status"] == "completed"
                                ])
                                status["failed_candidates"] = len([
                                    item for item in all_rows if item["status"] == "failed"
                                ])
                                status["infrastructure_failed_candidates"] = len([
                                    item for item in all_rows if item["status"] == "infrastructure_failed"
                                ])
                                atomic_write_json(status_path, status)
                            if retirement.get("process_still_alive"):
                                fatal_cleanup_error = (
                                    "persistent worker supervisor remained alive after cleanup failure"
                                )
                            elif retirement and retirement.get("termination_proven") is not True:
                                fatal_cleanup_error = (
                                    "persistent worker termination was not proven after cleanup failure"
                                )
                            elif (
                                retirement and retired_guard.get("ok", True)
                                and not retirement_cleanup.get("ok", True)
                            ):
                                fatal_cleanup_error = (
                                    "persistent worker epoch could not be safely removed"
                                )
                            elif (
                                cleanup_result.get("ok") is not True
                                and not safe_retirement
                                and not (retired_guard and retired_guard.get("ok") is False)
                            ):
                                fatal_cleanup_error = (
                                    "persistent worker candidate cleanup was not proven"
                                )
                            persistent_state["cleanup"] = public_cleanup_result(cleanup_result)
                            cleanup_changed = True
                    controller_termination = (
                        (row.get("persistent_worker") or {}).get("controller_termination") or {}
                    )
                    if (
                        candidate_is_persisted and row.get("status") == "infrastructure_failed"
                        and controller_termination.get("termination_proven") is True
                        and controller_termination.get("process_still_alive") is False
                    ):
                        persistent_state = row.setdefault("persistent_worker", {})
                        try:
                            candidate_cleanup = cleanup_proven_terminal_infrastructure_candidate(
                                candidate_root, run_root,
                                required_lightweight_paths=[candidate_path, os.path.join(
                                    candidate_root, "timing.json",
                                )],
                            )
                        except Exception as error:
                            persistent_state["candidate_cleanup"] = {
                                "ok": False, "state": "cleanup_exception",
                                "error": "%s: %s" % (error.__class__.__name__, error),
                            }
                            cleanup_changed = True
                            fatal_cleanup_error = (
                                "proven-dead controller candidate artifacts could not be safely removed"
                            )
                        else:
                            persistent_state["candidate_cleanup"] = candidate_cleanup
                            cleanup_request = None
                            cleanup_changed = True
                    if cleanup_request is not None:
                        candidate_cleanup = safe_delete_candidate_workspace(
                            cleanup_request[0], run_root, cleanup_request[1],
                            required_lightweight_paths=cleanup_request[2],
                        )
                        if cleanup_worker is not None and isinstance(candidate_cleanup, dict):
                            row.setdefault("persistent_worker", {})["candidate_cleanup"] = candidate_cleanup
                            cleanup_changed = True
                    if cleanup_changed:
                        data_store.commit_candidates([row])
                        if candidate_is_persisted:
                            persist_lightweight_candidate(candidate_root, row)
                        compact_after_cleanup = json.dumps({
                            "candidate": _pack_candidate(row, checkpoint_variable_ids),
                            "counts": [
                                status.get("completed_candidates", 0),
                                status.get("failed_candidates", 0),
                                status.get("infrastructure_failed_candidates", 0),
                            ],
                        }, separators=(",", ":"), sort_keys=True)
                        if compact_after_cleanup != compact_before_cleanup:
                            save_checkpoint(generation, active_plan)
                    if fatal_cleanup_error is not None:
                        raise RuntimeError(fatal_cleanup_error)
                    if row.get("status") == "completed":
                        native_ack = bool(cleanup_worker is not None and
                            (row.get("persistent_worker") or {}).get("cleanup_pending") is False)
                        cli_ack = bool(row.get("execution_backend") == "private_netlist"
                            and not row.get("cleanup_pending")
                            and (row.get("native_job_state") or {}).get("native_completion_proven") is True)
                        legacy_ack = bool(row.get("execution_backend") in ("legacy", "adexl")
                            and not process_cleanup_unproven and cleanup_request is not None)
                        if native_ack or cli_ack or legacy_ack:
                            fold_successful_candidate(row, "native_candidate_cleanup_ack" if native_ack else "owned_task_completion")
                    if health_limit_decision is not None:
                        if not resource_guard_is_recyclable(
                                health_limit_decision, "candidate_boundary"):
                            status["message"] = "%s; checkpoint saved for resume" % (
                                PersistentSessionHealthLimit(health_limit_decision),
                            )
                            status["persistent_session_health"] = health_limit_decision
                            atomic_write_json(status_path, status)
                            save_checkpoint(generation, active_plan)
                            raise PersistentSessionHealthLimit(health_limit_decision)
                        finished_after_row = set(
                            item.get("candidate_id") for item in all_rows
                        )
                        future_work = bool(
                            generation + 1 < generations or any(
                                "g%03d_c%04d" % (generation, index)
                                not in finished_after_row
                                for index, _values in active_plan["planned"]
                            )
                        )
                        if not future_work:
                            status["message"] = (
                                "persistent Maestro resource limit reached after final candidate; "
                                "normal final shutdown will reclaim the session"
                            )
                            status["persistent_session_health"] = health_limit_decision
                            atomic_write_json(status_path, status)
                            save_checkpoint(generation, active_plan)
                            continue
                        event, exhausted = begin_resource_guard_recycle(
                            generation, active_plan, cleanup_worker,
                            row["candidate_id"], "candidate_boundary",
                            health_limit_decision,
                        )
                        terminate_resource_guard_epoch(cleanup_worker, event)
                        if exhausted:
                            save_checkpoint(generation, active_plan)
                            raise PersistentSessionHealthLimit({
                                "state": "restart_limit_exhausted",
                                "restart_limit": resource_guard_restart_limit,
                                "candidate_id": row["candidate_id"],
                                "decision": health_limit_decision,
                            })
                        restart_resource_guard_epoch(
                            generation, active_plan, cleanup_worker, event,
                        )

                _raise_workspace_build_failure(generation, future_rows)
                _raise_source_integrity_failure(generation, future_rows)
                _raise_parameter_application_failure(generation, future_rows)
                startup_only = bool(future_rows) and all(
                    row.get("status") == "infrastructure_failed"
                    and row.get("failure_class") == "persistent_startup"
                    for row in future_rows
                )
                startup_streak = startup_streak + 1 if startup_only else 0
                _raise_repeated_persistent_startup_failure(
                    generation, future_rows, startup_streak, persistent_startup_limit,
                )
                return startup_streak

            while True:
                finished_ids = set(row.get("candidate_id") for row in all_rows)
                pending = [
                    (index, values) for index, values in active_plan["planned"]
                    if "g%03d_c%04d" % (generation, index) not in finished_ids
                ]
                if pending and batch_enabled:
                    worker = persistent_workers[0]
                    batch_fallback = False
                    for planned_batch in candidate_batches(pending, execution_plan["candidate_limit"]):
                        update_live_batch(planned_batch, generation, initializing=True)
                        request = worker.submit_evaluate_batch(planned_batch, generation, spec)
                        submitted_at = time.monotonic()
                        begin_expected_session_replacement(worker, submitted_at)
                        deadline = submitted_at + batch_timeout_seconds(len(planned_batch), spec, config)
                        next_health_sample = submitted_at + active_health_interval
                        status["active_batch_candidates"] = [
                            "g%03d_c%04d" % (generation, index) for index, _values in planned_batch
                        ]
                        status["message"] = "evaluating %d candidates in one Maestro batch" % len(planned_batch)
                        atomic_write_json(status_path, status)
                        result = None
                        next_live_sample = 0.0
                        while result is None:
                            if time.monotonic() >= next_live_sample:
                                next_live_sample = time.monotonic() + 1.0
                                update_live_batch(planned_batch, generation)
                            if os.path.exists(stop_path):
                                native_stop = getattr(worker, 'request_native_batch_stop', None)
                                if callable(native_stop):
                                    status['native_stop_before_log_retention'] = native_stop('user stop before diagnostic retention')
                                from .maestro_progress import preserve_live_logs
                                try:
                                    retained = preserve_live_logs(run_root, [live_candidate_rows[cid] for cid in
                                        status['active_batch_candidates'] if cid in live_candidate_rows])
                                    for retained_row in retained:
                                        retained_row['execution_progress']['stage'] = 'stopping'
                                        live_candidate_rows[retained_row['candidate_id']] = retained_row
                                    if retained:
                                        data_store.commit_candidates(retained)
                                except Exception as retention_error:
                                    status['live_log_retention_error'] = str(retention_error)
                                termination = worker.terminate_and_prove("user stop during Maestro candidate batch")
                                if termination.get('termination_proven'):
                                    cancelled = []
                                    for cid in status['active_batch_candidates']:
                                        live_row = live_candidate_rows.get(cid)
                                        if live_row:
                                            live_row['execution_progress']['stage'] = 'cancelled'
                                            for task in live_row['execution_progress'].get('tasks') or []:
                                                if task.get('state') not in ('finished', 'submission_failed'):
                                                    task['state'] = 'cancelled'
                                            cancelled.append(live_row)
                                    if cancelled:
                                        data_store.commit_candidates(cancelled)
                                status.setdefault("batch_stop_terminations", []).append(termination)
                                for index, values in planned_batch:
                                    recovered = _recover_terminal_candidate_wal(
                                        run_root, index, generation, values, spec,
                                        worker.worker_index, termination,
                                    )
                                    if recovered is not None:
                                        persistent_startup_streak = record_future_rows(
                                            recovered, None, persistent_startup_streak,
                                        )
                                stopped_reason = "user_stop"
                                status["message"] = "user_stop"
                                save_checkpoint(generation, active_plan)
                                atomic_write_json(status_path, status)
                                if not termination.get("termination_proven"):
                                    raise RuntimeError("batch stop was not proven; workspace retained: %s" % termination)
                                break
                            try:
                                result = worker.poll(request)
                                now = time.monotonic()
                                if result is None and now >= next_health_sample:
                                    next_health_sample = now + active_health_interval
                                    decision = observe_active_session_health(worker, worker.sample_health())
                                    if decision and decision.get("triggered"):
                                        status["persistent_session_health"] = decision
                                        raise PersistentSessionHealthLimit(decision)
                                if result is None and now >= deadline:
                                    raise PersistentControllerProcessError("bounded Maestro batch deadline exceeded")
                            except Exception as error:
                                from .maestro_progress import preserve_live_logs
                                try:
                                    retained = preserve_live_logs(run_root, [live_candidate_rows[cid] for cid in
                                        status['active_batch_candidates'] if cid in live_candidate_rows])
                                    for retained_row in retained:
                                        retained_row['execution_progress']['stage'] = 'infrastructure_failed'
                                        live_candidate_rows[retained_row['candidate_id']] = retained_row
                                    if retained:
                                        data_store.commit_candidates(retained)
                                except Exception as retention_error:
                                    status['live_log_retention_error'] = str(retention_error)
                                termination = worker.terminate_and_prove(str(error))
                                status.setdefault("batch_failure_terminations", []).append(termination)
                                status["message"] = str(error)
                                for index, values in planned_batch:
                                    recovered = _recover_terminal_candidate_wal(
                                        run_root, index, generation, values, spec,
                                        worker.worker_index, termination,
                                    )
                                    if recovered is not None:
                                        persistent_startup_streak = record_future_rows(
                                            recovered, None, persistent_startup_streak,
                                        )
                                save_checkpoint(generation, active_plan)
                                atomic_write_json(status_path, status)
                                # Never resubmit an ambiguous remote job or erase
                                # its private files merely because Ocean died.
                                raise
                            if result is None:
                                time.sleep(0.05)
                        if stopped_reason == "user_stop":
                            break
                        if not isinstance(result, dict):
                            raise RuntimeError("Maestro batch controller returned an invalid result")
                        if result.get("fallback"):
                            batch_enabled = False
                            batch_fallback = True
                            execution_plan.update({"effective_mode": "serial", "fallback": result["fallback"]})
                            status["maestro_candidate_execution"] = copy.deepcopy(execution_plan)
                            status["parallel_execution_policy"] = "serialized_single_maestro"
                            status["message"] = "batch unavailable; preserving serial execution: %s" % result["fallback"]
                            atomic_write_json(status_path, status)
                            break
                        update_live_batch(planned_batch, generation)
                        rows = result.get("rows")
                        expected_ids = set("g%03d_c%04d" % (generation, index) for index, _values in planned_batch)
                        if (not isinstance(rows, list) or len(rows) != len(expected_ids)
                                or any(not isinstance(row, dict) for row in rows)
                                or set(row.get("candidate_id") for row in rows) != expected_ids):
                            raise RuntimeError("Maestro batch returned an incomplete candidate set")
                        requested_by_id = dict(
                            ("g%03d_c%04d" % (generation, index), (index, values))
                            for index, values in planned_batch
                        )
                        for row in rows:
                            requested_index, requested_values = requested_by_id[row["candidate_id"]]
                            if (type(row.get("index")) is not int
                                    or row["index"] != requested_index
                                    or type(row.get("generation")) is not int
                                    or row["generation"] != generation
                                    or row.get("values") != requested_values):
                                raise RuntimeError("Maestro batch candidate identity/values changed: %s" % row["candidate_id"])
                        # Every candidate and the authoritative checkpoint must
                        # survive before discarding their shared native history.
                        persistent_startup_streak = record_future_rows(rows, None, persistent_startup_streak)
                        save_checkpoint(generation, active_plan)
                        cleanup = worker.commit_candidate(result["batch_id"])
                        status["last_batch_cleanup"] = cleanup
                        if not isinstance(cleanup, dict) or not cleanup.get("ok"):
                            save_checkpoint(generation, active_plan)
                            raise RuntimeError("Maestro batch cleanup could not be proven: %s" % cleanup)
                        for row in all_rows:
                            if (row.get("batch_execution") or {}).get("batch_id") == result["batch_id"]:
                                row.setdefault("persistent_worker", {})["cleanup_pending"] = False
                                fold_successful_candidate(row, "native_batch_cleanup_ack")
                        from .maestro_batch import compact_committed_batch
                        status["last_batch_artifact_compaction"] = compact_committed_batch(
                            run_root, result["batch_id"], cleanup,
                            batch_root=os.path.join(run_root, "batches", "g%03d_g%03d_c%04d" %
                                                    (generation, generation, planned_batch[0][0])))
                        decision = observe_session_health(worker, cleanup)
                        if decision and decision.get("triggered"):
                            status["persistent_session_health"] = decision
                            save_checkpoint(generation, active_plan)
                            raise PersistentSessionHealthLimit(decision)
                        authorize_active_session_health(worker, cleanup, decision)
                        save_checkpoint(generation, active_plan)
                        status.pop("active_batch_candidates", None)
                        atomic_write_json(status_path, status)
                    if stopped_reason == "user_stop":
                        break
                    if batch_fallback:
                        continue
                    # Re-evaluate pending/replacement planning with this same
                    # generation; no population padding to match maxjobs.
                    continue
                elif pending and persistent_backend and persistent_worker_lifetime == "run":
                    pending_iter = iter(pending)
                    active_requests = {}
                    retries = max(0, int(config["runtime"].get("infrastructure_retries", 1))) + 1
                    hard_seconds = (
                        retries * (
                            float(config["runtime"].get("persistent_workspace_prepare_timeout_seconds", 1800))
                            +
                            float(config["runtime"].get("ocean_startup_timeout_seconds", 90))
                            + serial_timeout_seconds(spec, config)
                        )
                        + scheduler_watchdog_grace
                    )

                    def submit_controller_request(worker, item):
                        index, values = item
                        update_live_batch([(index, values)], generation, initializing=True, serial=True)
                        request = worker.submit_evaluate(index, generation, values, spec)
                        submitted_at = time.monotonic()
                        begin_expected_session_replacement(worker, submitted_at)
                        active_requests[worker] = {
                            "request": request,
                            "index": index,
                            "values": values,
                            "deadline": submitted_at + hard_seconds,
                            "next_health_sample": submitted_at + active_health_interval,
                        }

                    for worker in persistent_workers[:min(len(pending), len(persistent_workers))]:
                        submit_controller_request(worker, next(pending_iter))
                    while active_requests:
                        if os.path.exists(stop_path):
                            stop_terminations = []
                            stop_recovered_rows = []
                            for worker, watched in list(active_requests.items()):
                                live_id = "g%03d_c%04d" % (generation, watched["index"])
                                native_stop = worker.request_native_batch_stop('user stop during Maestro serial candidate')
                                update_live_batch([(watched['index'], watched['values'])], generation, serial=True)
                                retained_live = None
                                if live_id in live_candidate_rows:
                                    from .maestro_progress import preserve_live_logs
                                    retained_live = preserve_live_logs(run_root, [live_candidate_rows[live_id]])[0]
                                    retained_live['execution_progress']['stage'] = 'stopping'
                                    live_candidate_rows[live_id] = retained_live
                                    data_store.commit_candidates([retained_live])
                                has_retained_candidate_log = bool(retained_live and any(
                                    task.get('retained_snapshot') for task in
                                    retained_live.get('execution_progress', {}).get('tasks') or []))
                                termination = worker.terminate_and_prove(
                                    "user stop requested during g%03d_c%04d"
                                    % (generation, watched["index"])
                                )
                                stop_termination = {
                                    "candidate_id": "g%03d_c%04d" % (generation, watched["index"]),
                                    "terminated_at": utc_timestamp(),
                                    "result": termination,
                                    "native_stop": native_stop,
                                }
                                stop_terminations.append(stop_termination)
                                status.setdefault("controller_stop_terminations", []).append(stop_termination)
                                status["controller_stop_terminations"] = status["controller_stop_terminations"][-20:]
                                atomic_write_json(status_path, status)
                                if not termination.get("termination_proven"):
                                    raise RuntimeError(
                                        "persistent controller termination was not proven during stop: %s"
                                        % termination
                                    )
                                if retained_live is not None:
                                    retained_live['execution_progress']['stage'] = 'cancelled'
                                    live_candidate_rows[live_id] = retained_live
                                    data_store.commit_candidates([retained_live])
                                recovered = _recover_terminal_candidate_wal(
                                    run_root, watched["index"], generation, watched["values"],
                                    spec, worker.worker_index, termination,
                                )
                                if recovered is not None:
                                    stop_termination["terminal_wal_recovery"] = recovered["wal_recovery"]
                                    stop_recovered_rows.append(recovered)
                                else:
                                    candidate_root = os.path.join(
                                        run_root, "candidates", stop_termination["candidate_id"],
                                    )
                                    try:
                                        candidate_cleanup = (
                                            cleanup_abandoned_nonterminal_candidate(
                                                candidate_root, run_root,
                                                verify_sources_unchanged,
                                            )
                                            if os.path.lexists(candidate_root) and not has_retained_candidate_log
                                            else {
                                                "ok": True, "removed": False,
                                                "state": "bounded_live_diagnostics_retained" if has_retained_candidate_log else "candidate_root_absent",
                                            }
                                        )
                                    except Exception as cleanup_error:
                                        stop_termination["candidate_cleanup"] = {
                                            "ok": False,
                                            "error": "%s: %s" % (
                                                cleanup_error.__class__.__name__, cleanup_error,
                                            ),
                                        }
                                        status["controller_stop_terminations"] = (
                                            status["controller_stop_terminations"][-20:]
                                        )
                                        atomic_write_json(status_path, status)
                                        raise RuntimeError(
                                            "user stop could not safely reclaim active candidate: %s"
                                            % cleanup_error
                                        )
                                    stop_termination["candidate_cleanup"] = candidate_cleanup
                            status["controller_stop_terminations"] = (
                                status["controller_stop_terminations"][-20:]
                            )
                            atomic_write_json(status_path, status)
                            for recovered in stop_recovered_rows:
                                persistent_startup_streak = record_future_rows(
                                    recovered, None, persistent_startup_streak,
                                )
                            active_requests.clear()
                            break
                        completed_requests = []
                        now = time.monotonic()
                        for worker, watched in list(active_requests.items()):
                            if now >= watched.get('next_live_sample', 0):
                                watched['next_live_sample'] = now + 1.
                                update_live_batch([(watched['index'], watched['values'])], generation, serial=True)
                            result = None
                            failure = None
                            termination = None
                            resource_decision = None
                            try:
                                result = worker.poll(watched["request"])
                            except Exception as error:
                                failure = error
                            if (
                                result is None and failure is None
                                and now >= watched["next_health_sample"]
                            ):
                                watched["next_health_sample"] = now + active_health_interval
                                sampler = getattr(worker, "sample_health", None)
                                if callable(sampler):
                                    try:
                                        active_health = sampler()
                                        if not isinstance(active_health, dict):
                                            raise RuntimeError(
                                                "active health sampler returned a non-object"
                                            )
                                    except Exception as health_error:
                                        active_health = {
                                            "alive": True,
                                            "tree_complete": False,
                                            "storage_complete": False,
                                            "logs_complete": False,
                                            "sampling_error": "%s: %s" % (
                                                health_error.__class__.__name__, health_error,
                                            ),
                                        }
                                    resource_decision = observe_active_session_health(
                                        worker, active_health,
                                    )
                                    watched["last_health_decision"] = resource_decision
                                    if not (
                                        resource_decision
                                        and resource_decision.get("triggered")
                                    ):
                                        resource_decision = None
                            if resource_decision is not None:
                                resource_error = PersistentSessionHealthLimit(resource_decision)
                                candidate_id = "g%03d_c%04d" % (
                                    generation, watched["index"],
                                )
                                if not resource_guard_is_recyclable(
                                        resource_decision, "active_candidate"):
                                    termination = worker.terminate_and_prove(str(resource_error))
                                    resource_record = {
                                        "candidate_id": candidate_id,
                                        "requested_at": utc_timestamp(),
                                        "decision": resource_decision,
                                        "result": termination,
                                    }
                                    status["message"] = (
                                        "persistent Maestro health guard exceeded; "
                                        "checkpoint saved for resume"
                                    )
                                    status["persistent_session_health"] = resource_decision
                                    status.setdefault(
                                        "active_resource_guard_terminations", [],
                                    ).append(resource_record)
                                    status["active_resource_guard_terminations"] = (
                                        status["active_resource_guard_terminations"][-20:]
                                    )
                                    atomic_write_json(status_path, status)
                                    recovered = None
                                    if termination.get("termination_proven"):
                                        recovered = _recover_terminal_candidate_wal(
                                            run_root, watched["index"], generation,
                                            watched["values"], spec, worker.worker_index,
                                            termination,
                                        )
                                    if recovered is not None:
                                        resource_record["terminal_wal_recovery"] = (
                                            recovered["wal_recovery"]
                                        )
                                    resource_row = recovered or _persistent_controller_failure_candidate(
                                        run_root, watched["index"], generation,
                                        watched["values"], spec, worker.worker_index,
                                        resource_error, termination,
                                        failure_class="resource_guard",
                                    )
                                    resource_row.setdefault("persistent_worker", {}).update({
                                        "active_session_health": resource_decision,
                                        "controller_termination": termination,
                                    })
                                    active_requests.pop(worker, None)
                                    persistent_startup_streak = record_future_rows(
                                        resource_row, None, persistent_startup_streak,
                                    )
                                    save_checkpoint(generation, active_plan)
                                    if not termination.get("termination_proven"):
                                        raise RuntimeError(
                                            "active health guard could not prove controller termination: %s"
                                            % termination
                                        )
                                    raise resource_error
                                resource_record, exhausted = begin_resource_guard_recycle(
                                    generation, active_plan, worker, candidate_id,
                                    "active_candidate", resource_decision,
                                )
                                termination = terminate_resource_guard_epoch(
                                    worker, resource_record,
                                )
                                recovered = None
                                recovered = _recover_terminal_candidate_wal(
                                    run_root, watched["index"], generation,
                                    watched["values"], spec, worker.worker_index,
                                    termination,
                                )
                                if recovered is not None:
                                    resource_record["terminal_wal_recovery"] = recovered["wal_recovery"]
                                    active_requests.pop(worker, None)
                                    persistent_startup_streak = record_future_rows(
                                        recovered, None, persistent_startup_streak,
                                    )
                                else:
                                    candidate_root = os.path.join(
                                        run_root, "candidates", candidate_id,
                                    )
                                    try:
                                        candidate_cleanup = (
                                            cleanup_abandoned_nonterminal_candidate(
                                                candidate_root, run_root,
                                                verify_sources_unchanged,
                                            )
                                            if os.path.lexists(candidate_root)
                                            else {
                                                "ok": True, "removed": False,
                                                "state": "candidate_root_absent",
                                            }
                                        )
                                    except Exception as cleanup_error:
                                        resource_record["candidate_cleanup"] = {
                                            "ok": False,
                                            "error": "%s: %s" % (
                                                cleanup_error.__class__.__name__, cleanup_error,
                                            ),
                                        }
                                        atomic_write_json(status_path, status)
                                        raise RuntimeError(
                                            "resource guard could not safely reclaim active candidate: %s"
                                            % cleanup_error
                                        )
                                    resource_record["candidate_cleanup"] = candidate_cleanup
                                    active_requests.pop(worker, None)
                                atomic_write_json(status_path, status)
                                save_checkpoint(generation, active_plan)
                                if exhausted:
                                    raise PersistentSessionHealthLimit({
                                        "state": "restart_limit_exhausted",
                                        "restart_limit": resource_guard_restart_limit,
                                        "candidate_id": candidate_id,
                                        "decision": resource_decision,
                                    })
                                restarted = restart_resource_guard_epoch(
                                    generation, active_plan, worker, resource_record,
                                )
                                if not restarted:
                                    continue
                                if recovered is None:
                                    submit_controller_request(
                                        worker, (watched["index"], watched["values"]),
                                    )
                                else:
                                    try:
                                        item = next(pending_iter)
                                    except StopIteration:
                                        pass
                                    else:
                                        submit_controller_request(worker, item)
                                continue
                            if result is None and failure is None and now >= watched["deadline"]:
                                failure = PersistentControllerProcessError(
                                    "controller hard deadline exceeded after %.1f seconds" % hard_seconds
                                )
                            if failure is not None:
                                fatal_remote = bool(
                                    isinstance(failure, PersistentControllerRemoteError)
                                    and failure.fatal_run
                                )
                                termination = worker.terminate_and_prove(str(failure))
                                candidate_id = "g%03d_c%04d" % (
                                    generation, watched["index"],
                                )
                                termination_record = {
                                    "candidate_id": candidate_id,
                                    "requested_at": utc_timestamp(),
                                    "error": "%s: %s" % (failure.__class__.__name__, failure),
                                    "result": termination,
                                }
                                if fatal_remote:
                                    termination_record.update({
                                        "remote_error_type": failure.remote_error_type,
                                        "failure_class": failure.failure_class,
                                    })
                                    status["message"] = (
                                        "external EDA environment changed; checkpoint saved for resume"
                                    )
                                    status.setdefault(
                                        "external_environment_terminations", [],
                                    ).append(termination_record)
                                    status["external_environment_terminations"] = (
                                        status["external_environment_terminations"][-20:]
                                    )
                                else:
                                    status["message"] = (
                                        "watchdog terminated controller for %s" % candidate_id
                                    )
                                    status.setdefault(
                                        "scheduler_watchdog_terminations", [],
                                    ).append(termination_record)
                                    status["scheduler_watchdog_terminations"] = (
                                        status["scheduler_watchdog_terminations"][-20:]
                                    )
                                atomic_write_json(status_path, status)
                                if not termination.get("termination_proven"):
                                    raise RuntimeError(
                                        "persistent controller termination was not proven: %s" % termination
                                    )
                                if fatal_remote:
                                    active_requests.pop(worker, None)
                                    save_checkpoint(generation, active_plan)
                                    candidate_root = os.path.join(
                                        run_root, "candidates", candidate_id,
                                    )
                                    try:
                                        candidate_cleanup = (
                                            cleanup_abandoned_nonterminal_candidate(
                                                candidate_root, run_root,
                                                verify_sources_unchanged,
                                            )
                                            if os.path.lexists(candidate_root)
                                            else {
                                                "ok": True, "removed": False,
                                                "state": "candidate_root_absent",
                                            }
                                        )
                                    except Exception as cleanup_error:
                                        termination_record["candidate_cleanup"] = {
                                            "ok": False,
                                            "error": "%s: %s" % (
                                                cleanup_error.__class__.__name__, cleanup_error,
                                            ),
                                        }
                                        atomic_write_json(status_path, status)
                                        raise RuntimeError(
                                            "external EDA environment changed and the active "
                                            "candidate could not be safely reclaimed: %s"
                                            % cleanup_error
                                        )
                                    termination_record["candidate_cleanup"] = candidate_cleanup
                                    atomic_write_json(status_path, status)
                                    raise ExternalEdaEnvironmentChanged(str(failure))
                                result = _recover_terminal_candidate_wal(
                                    run_root, watched["index"], generation, watched["values"],
                                    spec, worker.worker_index, termination,
                                )
                                if result is not None:
                                    termination_record["terminal_wal_recovery"] = result["wal_recovery"]
                                    status["message"] = "recovered terminal WAL for %s" % result["candidate_id"]
                                    atomic_write_json(status_path, status)
                                else:
                                    result = _persistent_controller_failure_candidate(
                                        run_root, watched["index"], generation, watched["values"], spec,
                                        worker.worker_index, failure, termination,
                                    )
                            if result is not None:
                                completed_requests.append((watched["index"], worker, result, failure))
                        if not completed_requests:
                            time.sleep(0.05)
                            continue
                        for _index, worker, result, failure in sorted(completed_requests):
                            active_requests.pop(worker, None)
                            persistent_startup_streak = record_future_rows(
                                result, None if failure is not None else worker,
                                persistent_startup_streak,
                            )
                            if failure is not None and not os.path.exists(stop_path):
                                worker.restart()
                                expect_session_replacement(worker)
                            if os.path.exists(stop_path):
                                continue
                            try:
                                item = next(pending_iter)
                            except StopIteration:
                                continue
                            submit_controller_request(worker, item)
                elif pending:
                    with ThreadPoolExecutor(max_workers=jobs) as executor:
                        pending_iter = iter(pending)
                        active_futures = {}

                        def submit_next_candidate():
                            try:
                                index, values = next(pending_iter)
                            except StopIteration:
                                return False
                            if netlist_optimizer is not None:
                                future = executor.submit(netlist_optimizer.evaluate, index, generation, values)
                            else:
                                future = executor.submit(
                                    _evaluate_candidate, run_root, index, generation, values,
                                    spec, prepared, selected, config,
                                )
                            active_futures[future] = (index, values)
                            return True

                        for _unused in range(min(jobs, len(pending))):
                            submit_next_candidate()
                        try:
                            while active_futures:
                                wait_options = {"return_when": FIRST_COMPLETED}
                                if netlist_optimizer is not None:
                                    wait_options["timeout"] = 0.2
                                completed, _waiting = wait(
                                    set(active_futures), **wait_options
                                )
                                if netlist_optimizer is not None:
                                    drain_point_commits(generation)
                                if netlist_optimizer is not None and os.path.exists(stop_path):
                                    netlist_optimizer.cancel()
                                completed_count = 0
                                for future in sorted(
                                        completed,
                                        key=lambda item: active_futures[item][0]):
                                    active_futures.pop(future)
                                    persistent_startup_streak = record_future_rows(
                                        future.result(), None,
                                        persistent_startup_streak,
                                    )
                                    completed_count += 1
                                if (netlist_optimizer is not None and netlist_optimizer.fatal_error
                                        and not os.path.exists(stop_path)):
                                    netlist_optimizer.cancel()
                                    # Persist every already-created candidate before failing the run.
                                    for future in sorted(active_futures, key=lambda item: active_futures[item][0]):
                                        record_future_rows(future.result(), None, persistent_startup_streak)
                                    active_futures.clear()
                                    raise PrivateNetlistRunError(netlist_optimizer.fatal_error)
                                if not os.path.exists(stop_path):
                                    for _unused in range(completed_count):
                                        submit_next_candidate()
                        except Exception:
                            if netlist_optimizer is not None:
                                netlist_optimizer.cancel()
                            for future in active_futures:
                                future.cancel()
                            raise
                if os.path.exists(stop_path):
                    stopped_reason = "user_stop"
                    status["message"] = "user_stop"
                    atomic_write_json(status_path, status)
                    save_checkpoint(generation, active_plan)
                    break
                finished_ids = set(row.get("candidate_id") for row in all_rows)
                remaining_planned = [
                    (index, values) for index, values in active_plan["planned"]
                    if "g%03d_c%04d" % (generation, index) not in finished_ids
                ]
                if remaining_planned and not os.path.exists(stop_path):
                    continue
                missing = population - len(generation_rows)
                allowance = replacement_limit - active_plan["replacements"]
                if missing > 0 and allowance > 0 and not os.path.exists(stop_path):
                    count = min(missing, allowance)
                    replacement_values = generator.ask(count)
                    first_index = active_plan["next_index"]
                    active_plan["planned"].extend(
                        (first_index + offset, values) for offset, values in enumerate(replacement_values)
                    )
                    active_plan["next_index"] += count
                    active_plan["replacements"] += count
                    status["total_candidates"] += count
                    status["message"] = "resampling %d infrastructure-failed candidate(s)" % count
                    atomic_write_json(status_path, status)
                    save_checkpoint(generation, active_plan)
                    continue
                break
            if netlist_optimizer is not None:
                netlist_optimizer.verify_sources()
            if stopped_reason == "user_stop":
                break
            if not generation_rows and infrastructure_rows:
                raise RuntimeError(
                    "systemic infrastructure failure in generation %d; optimization stopped instead of "
                    "replacing candidates indefinitely: %s" % (
                        generation + 1, _infrastructure_failure_summary(infrastructure_rows),
                    )
                )
            generation_rows.sort(key=lambda row: (
                int(row.get("index", 0)), str(row.get("candidate_id") or ""),
            ))
            generation_accepted = generator.tell(generation_rows)
            completed_generation = [row for row in generation_rows if row["status"] == "completed"]
            if not completed_generation:
                status["message"] = "generation %d had no valid candidate; retained prior population" % (generation + 1)
                atomic_write_json(status_path, status)
                active_plan = None
                save_checkpoint(generation + 1, active_plan)
                continue
            survivor_population = [row for row in generator.previous if row.get("status") == "completed"]
            representative, ideal_distance = select_representative(survivor_population or completed_generation)
            selection_history.append({
                "generation": generation,
                "candidate_id": representative["candidate_id"],
                "role": "single_objective_best" if objective_count == 1 else "pareto_knee_representative",
                "total_violation": representative["metrics"]["total_violation"],
                "objective_scores": [row["score"] for row in representative["metrics"]["objectives"]],
                "merit": representative["metrics"]["merit"] if objective_count == 1 else None,
                "ideal_distance": ideal_distance if objective_count > 1 else None,
            })
            auto_stop = spec.get("auto_stop", {}) if generation_accepted and objective_count == 1 else {}
            patience = int(auto_stop.get("patience", 0))
            tolerance = float(auto_stop.get("tolerance", 0.0))
            if patience > 0 and len(selection_history) > patience:
                recent = selection_history[-patience - 1:]
                improvement = recent[0]["merit"] - min(row["merit"] for row in recent[1:])
                if improvement <= tolerance and all(row["total_violation"] <= 0 for row in recent):
                    stopped_reason = "auto_stop_no_improvement"
                    active_plan = None
                    save_checkpoint(
                        generation + 1, active_plan, terminal_state="completed",
                        terminal_reason=stopped_reason,
                    )
                    break
            active_plan = None
            if generation + 1 >= generations:
                stopped_reason = "generation_limit"
                save_checkpoint(
                    generation + 1, active_plan, terminal_state="completed",
                    terminal_reason=stopped_reason,
                )
            else:
                save_checkpoint(generation + 1, active_plan)
        completed = [row for row in all_rows if row["status"] == "completed"]
        if not completed and stopped_reason != "user_stop":
            raise RuntimeError("all %d candidates failed across %d generations; inspect candidate errors" % (len(all_rows), generations))
        if stopped_reason != "user_stop" and not terminal_checkpoint_written:
            stopped_reason = "generation_limit"
            save_checkpoint(
                generations, None, terminal_state="completed",
                terminal_reason=stopped_reason,
            )
        front = nondominated_front(completed) if completed else []
        representative, ideal_distance = select_representative(completed) if completed else (None, None)
        result_state = "stopped" if stopped_reason == "user_stop" else "completed"
        result = {
            "schema_version": "crescendo.run_result.v2",
            "run_id": status["run_id"],
            "run_root": run_root,
            "display_name": status["display_name"],
            "state": result_state,
            "stopped_reason": stopped_reason or "generation_limit",
            "algorithm": algorithm,
            "algorithm_policy": algorithm_policy,
            "algorithm_parameters": algorithm_parameters,
            "representative": representative,
            "representative_role": "single_objective_best" if objective_count == 1 else "pareto_knee_representative",
            "representative_ideal_distance": ideal_distance if objective_count > 1 else None,
            "pareto_front": front,
            "candidates": all_rows,
            "selection_history": selection_history,
            "raw_cleanup": {"policy": "zero_raw_retention", "kept": []},
            "finished_at": utc_timestamp(),
        }
        if objective_count == 1 and representative is not None:
            result["best"] = representative
        if representative is not None:
            assignment_name = "best_assignment.json" if objective_count == 1 else "representative_assignment.json"
            pending_assignment = (assignment_name, {
                "candidate_id": representative["candidate_id"],
                "role": result["representative_role"],
                "values": representative["values"],
                "metrics": representative["metrics"],
            })
        status.update({"state": "finalizing", "message": (
            "closing owned Spectre tasks" if netlist_optimizer is not None else "closing persistent Maestro session"
        )})
        atomic_write_json(status_path, status)
        pending_result = result
    except Exception as error:
        message = "%s: %s" % (error.__class__.__name__, error)
        status.setdefault("primary_failure", {
            "error_type": error.__class__.__name__, "message": str(error),
            "failure_stage": getattr(error, "startup_stage", None) or "optimization",
            "diagnostic_path": getattr(error, "startup_diagnostic_path", None),
        })
        shared_failure = getattr(netlist_optimizer, "preparation_failure", None)
        if isinstance(shared_failure, dict):
            status["shared_preparation_failure"] = copy.deepcopy(shared_failure)
            status["primary_failure"]["failure_stage"] = shared_failure.get("failure_stage") or "native_template_preparation"
            status["primary_failure"]["diagnostic_path"] = shared_failure.get("diagnostic_path")
        status.update({"state": "failed", "message": message, "finished_at": utc_timestamp()})
        try:
            data_store.append_event("status", status)
        except Exception:
            pass  # Preserve the first error; failed writes never allow raw deletion.
        atomic_write_json(status_path, status)
        raise
    finally:
        active_exception = sys.exc_info()[0] is not None
        shutdown = []
        if netlist_optimizer is not None:
            try:
                close_result = netlist_optimizer.close(cancel=active_exception or stopped_reason == "user_stop")
                shutdown.append({"worker_index": "private_netlist", "ok": close_result.get("cleanup_proven") is True,
                                 "result": close_result})
                if pending_result is not None:
                    pending_result["raw_cleanup"] = {
                        "policy": "candidate_raw_removed_after_checkpoint; shared_template_retained",
                        "kept": [os.path.relpath(netlist_optimizer.root, run_root)],
                    }
            except Exception as error:
                shutdown.append({"worker_index": "private_netlist", "ok": False,
                                 "error": "%s: %s" % (error.__class__.__name__, error)})
        for worker in persistent_workers:
            try:
                close_result = worker.close()
                cleanup_result = close_result.get("cleanup") or {}
                shutdown.append({
                    "worker_index": worker.worker_index,
                    "ok": bool(
                        close_result.get("termination_proven") is True
                        and close_result.get("process_still_alive") is False
                        and cleanup_result.get("ok") is True
                        and (cleanup_result.get("removed") is True
                             or cleanup_result.get("state") in ("already_removed", "no_owned_epoch"))
                    ),
                    "result": close_result,
                })
            except Exception as error:
                shutdown.append({
                    "worker_index": worker.worker_index,
                    "ok": False,
                    "error": "%s: %s" % (error.__class__.__name__, error),
                })
        audit_error = None
        if persistent_workers or netlist_optimizer is not None:
            try:
                shutdown_document = {
                    "schema_version": "crescendo.worker_shutdown.v1",
                    "workers": shutdown,
                    "finished_at": utc_timestamp(),
                }
            except Exception as error:
                audit_error = "%s: %s" % (error.__class__.__name__, error)
        if persistent_workers:
            from .retention import inspect_run_cleanup_scope
            scope = inspect_run_cleanup_scope(run_root)
            process_proven = all((item.get('result') or {}).get('process_termination_proven') is True
                                 for item in shutdown)
            remote_proven = all((item.get('result') or {}).get('remote_stop_proven') is True
                                for item in shutdown)
            final_artifacts_proven = bool(scope.get('artifact_cleanup_complete') is True
                and process_proven and remote_proven and all(item.get('ok') is True for item in shutdown))
            status['final_cleanup'] = {'process_termination_proven': process_proven,
                'remote_stop_proven': remote_proven,
                'artifact_cleanup_complete': final_artifacts_proven,
                'run_cleanup_scope': scope, 'observed_at': utc_timestamp()}
            earlier_attempts = list(status.get('batch_stop_terminations') or []) + list(status.get('batch_failure_terminations') or [])
            earlier_attempts.extend(item.get('result') or {} for item in status.get('controller_stop_terminations') or [])
            for earlier in earlier_attempts:
                if earlier.get('artifact_cleanup_complete') is False:
                    earlier['historical_attempt'] = True
                    earlier['final_artifact_cleanup_complete'] = final_artifacts_proven
                    earlier['earlier_failure_recovered'] = final_artifacts_proven
                    earlier['resolved_by_final_cleanup'] = {'ok': final_artifacts_proven,
                        'observed_at': status['final_cleanup']['observed_at']}
            if scope.get('artifact_cleanup_complete') is not True:
                shutdown.append({'worker_index': 'run_artifact_scope', 'ok': False,
                    'error': 'owned run artifacts remain; earlier cleanup proof does not cover them',
                    'result': scope})
            try:
                atomic_write_json(status_path, status)
                atomic_write_json(os.path.join(run_root, 'worker_shutdown.json'), {
                    'schema_version': 'crescendo.worker_shutdown.v1', 'workers': shutdown,
                    'final_cleanup': status['final_cleanup'], 'finished_at': utc_timestamp()})
            except Exception as final_audit_error:
                audit_error = str(final_audit_error)
        elif netlist_optimizer is not None:
            try:
                atomic_write_json(os.path.join(run_root, 'worker_shutdown.json'), shutdown_document)
            except Exception as final_audit_error:
                audit_error = str(final_audit_error)
        shutdown_failures = [row for row in shutdown if not row.get("ok")]
        if shutdown_failures or audit_error:
            message = "owned simulation worker shutdown/cleanup failed"
            if audit_error:
                message += "; audit write failed: %s" % audit_error
            failed_result_error = None
            if pending_result is not None:
                failed_result = copy.deepcopy(pending_result)
                failed_result["state"] = "failed"
                failed_result["stopped_reason"] = "persistent_worker_shutdown_failure"
                failed_result["worker_shutdown_failures"] = shutdown_failures
                if audit_error:
                    failed_result["worker_shutdown_audit_error"] = audit_error
                try:
                    try:
                        data_store.commit_result(failed_result)
                    except Exception:
                        pass
                    publish_result_summary(failed_result)
                except Exception as error:
                    failed_result_error = "%s: %s" % (error.__class__.__name__, error)
                    message += "; failed result write failed: %s" % failed_result_error
            status["cleanup_failure"] = {
                "message": message, "workers": shutdown_failures,
                "audit_error": audit_error, "failed_result_error": failed_result_error,
                "recovery_required": True,
            }
            status["secondary_cleanup_error"] = message
            if not active_exception:
                status.setdefault("primary_failure", {
                    "error_type": "RuntimeError", "message": message,
                    "failure_stage": "worker_cleanup",
                })
                status["message"] = message
            status.update({"state": "failed", "finished_at": utc_timestamp()})
            append_run_event(run_root, "cleanup_failed", message, status["cleanup_failure"], level="ERROR")
            try:
                atomic_write_json(status_path, status)
            except Exception:
                pass
            if not active_exception:
                raise RuntimeError(message)

    if pending_result is None:
        raise RuntimeError("optimization finalization produced no terminal result")
    try:
        data_store.commit_result(pending_result)
        if pending_assignment is not None:
            atomic_write_json(
                os.path.join(run_root, pending_assignment[0]), pending_assignment[1],
            )
        publish_result_summary(pending_result)
        status.update({
            "state": pending_result["state"],
            "message": pending_result["stopped_reason"],
            "finished_at": pending_result["finished_at"],
        })
        atomic_write_json(status_path, status)
    except Exception as error:
        message = "terminal result publication failed: %s: %s" % (
            error.__class__.__name__, error,
        )
        failed_result = copy.deepcopy(pending_result)
        failed_result["state"] = "failed"
        failed_result["stopped_reason"] = "terminal_result_publication_failure"
        failed_result["publication_error"] = message
        try:
            publish_result_summary(failed_result)
        except Exception:
            pass
        status.update({"state": "failed", "message": message, "finished_at": utc_timestamp()})
        try:
            atomic_write_json(status_path, status)
        except Exception:
            pass
        raise
    return pending_result
