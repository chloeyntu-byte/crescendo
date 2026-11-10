; Crescendo captures the invoking ADE window at click time; it never saves the setup.
; A project loader can set crescPluginLauncher before loading this file.
unless(boundp('crescPluginLauncher) && stringp(crescPluginLauncher)
  crescPluginSourceFile = simplifyFilename(get_filename(piport))
  crescPluginSourceParts = parseString(crescPluginSourceFile "/")
  crescPluginSourceDir = strcat("/" buildString(reverse(cdr(reverse(crescPluginSourceParts))) "/"))
  crescPluginLauncher = strcat(crescPluginSourceDir "/crescendo_launch")
)

; POSIX shell quoting also preserves dollar signs and apostrophes in paths.
procedure(crescMaestroShellQuote(value)
  let((quoted index character)
    quoted = "'"
    for(index 1 strlen(value)
      character = substring(value index 1)
      quoted = strcat(quoted if(character == "'" then "'\\''" else character))
    )
    strcat(quoted "'")
  )
)

unless(boundp('crescMaestroLaunchJobs)
  crescMaestroLaunchJobs = makeTable('crescMaestroLaunchJobs nil)
)

; IPC pre-expands variables and quotes. Keep every dynamic argument inside an
; owned temporary script; only its generated ASCII path enters the IPC command.
procedure(crescMaestroWriteLaunchScript(context)
  let((script port outcome)
    script = makeTempFileName("/tmp/crescendo_cmd_XXXXXX")
    unless(rexMatchp("^/tmp/crescendo_cmd_[A-Za-z0-9_]+$" script)
      when(isFile(script) deleteFile(script))
      error("Crescendo: cannot create a safe local IPC transport path.\n")
    )
    ; makeTempFileName creates the owned file with private permissions.
    port = outfile(script)
    unless(port
      when(isFile(script) deleteFile(script))
      error("Crescendo: cannot write the private IPC launch script.\n")
    )
    outcome = errset(
      progn(
        fprintf(port "#!/bin/bash\n/bin/rm -f -- \"$0\"\nexec %s %s\n"
          crescMaestroShellQuote(crescPluginLauncher) crescMaestroShellQuote(context))
        t
      ) t)
    close(port)
    unless(outcome && car(outcome)
      when(isFile(script) deleteFile(script))
      error("Crescendo: private IPC launch script export failed.\n")
    )
    script
  )
)

procedure(crescMaestroLaunchOutput(_process data)
  when(stringp(data) && data != "" printf("%s" data))
)

procedure(crescMaestroLaunchExit(process status)
  let((paths)
    paths = crescMaestroLaunchJobs[process]
    foreach(path paths when(isFile(path) deleteFile(path)))
    remove(process crescMaestroLaunchJobs)
  )
  unless(equal(status 0)
    printf("Crescendo launch process exited with status %L; see the reported launch log.\n" status)
  )
)

procedure(crescMaestroCurrentCdsLib()
  let((value result)
    value = nil
    ; The loaded mapping takes precedence over a later startup search.
    when(isCallable('ddGetUpdatedLib)
      result = errset(ddGetUpdatedLib() t)
      when(result && stringp(car(result)) && car(result) != ""
        value = car(result)
      )
    )
    unless(value
      when(isCallable('ddGetForcedLib)
        result = errset(ddGetForcedLib() t)
        when(result && stringp(car(result)) && car(result) != ""
          value = car(result)
        )
      )
    )
    unless(value
      when(isCallable('ddGetStartup)
        result = errset(ddGetStartup("cds.lib") t)
        when(result && stringp(car(result)) && car(result) != ""
          value = car(result)
        )
      )
    )
    value
  )
)


procedure(crescWriteOneJobPolicy(port sess scope testName @optional identicalPolicy)
  let((result policy cursor key value)
    result = nil
    when(isCallable('maeGetJobPolicy)
      result = errset(if(testName then maeGetJobPolicy(?session sess ?testName testName)
                                 else maeGetJobPolicy(?session sess)) nil)
    )
    policy = if(result then car(result) else nil)
    fprintf(port "P\t%L\t%L\t%L\n" scope testName if(policy then t else nil))
    if(policy && identicalPolicy && equal(policy identicalPolicy) then
      ; This is equality of actual returned values, not an inheritance claim.
      fprintf(port "D\t%L\t%L\n" scope testName)
    else when(policy
      cursor = cdr(policy)
      while(cursor && cdr(cursor)
        key = car(cursor)
        value = cadr(cursor)
        when(symbolp(key)
          if(stringp(value) || numberp(value) || value == nil || value == t then
            fprintf(port "J\t%L\t%L\t%L\t%L\n" scope testName symbolToString(key)
              if(numberp(value) then sprintf(nil "%L" value) else value))
          else
            fprintf(port "O\t%L\t%L\t%L\t%L\n" scope testName symbolToString(key)
              if(listp(value) then "list" else if(symbolp(value) then "symbol" else "native_object")))
          )
        )
        cursor = cddr(cursor)
      )
    ))
    policy
  )
)
procedure(crescWriteJobPolicyRecords(port sess testNames)
  let((result globalPolicy value item)
    globalPolicy = crescWriteOneJobPolicy(port sess "global" nil)
    foreach(name testNames crescWriteOneJobPolicy(port sess "test" name globalPolicy))
    result = nil
    when(isCallable('maeGetCurrentRunMode)
      result = errset(maeGetCurrentRunMode(?session sess) nil)
    )
    fprintf(port "Q\t%L\n" if(result && stringp(car(result)) then car(result) else nil))
    result = nil
    when(isCallable('axlGetRunDistributeOptions)
      result = errset(axlGetRunDistributeOptions(axlGetActiveSetup(axlGetMainSetupDB(sess))) nil)
    )
    foreach(key '(RunIn DivideJobs)
      value = if(result && car(result) then get(car(result) key) else nil)
      when(symbolp(value) && value != nil && value != t value = symbolToString(value))
      fprintf(port "U\t%L\t%L\t%L\n" symbolToString(key)
        if(result && car(result) then t else nil) value)
    )
    when(result && car(result)
      foreach(item car(result)->JobLimit
        fprintf(port "U\t%L\tt\t%L\n" strcat("JobLimit:" car(item)) cadr(item))
      )
    )
  )
)
procedure(crescWriteStorageReadback(port hsdb)
  let((result)
    result = nil
    when(isCallable('axlGetResultsLocation) result = errset(axlGetResultsLocation(hsdb) t))
    fprintf(port "V\t%L\t%L\t%L\n" "results_location" if(result && stringp(car(result)) then t else nil) if(result then car(result) else nil))
  )
)
procedure(crescWriteSaveOptionsRecords(port hsdb)
  let((result)
    foreach(row '(("save_res_dir" "adexl.results" "saveResDir")
                  ("save_dir" "adexl.results" "saveDir")
                  ("project_dir" "asimenv.startup" "projectDir")
                  ("fallback" "adexl.results" "fallbackToDirWhenSimResultsUnspecified")
                  ("use_local_psf_dir" "adexl.results" "useLocalPsfDir")
                  ("use_local_rdb_dir" "adexl.results" "useLocalRdbDir")
                  ("local_psf_dir" "adexl.results" "saveLocalPsfDir")
                  ("local_rdb_dir" "adexl.results" "saveLocalRdbDir"))
      result = errset(envGetVal(cadr(row) caddr(row)) nil)
      fprintf(port "V\t%L\t%L\t%L\n" car(row) if(result then t else nil) if(result then car(result) else nil))
    )
    fprintf(port "V\t%L\tt\t%L\n" "source_home" getShellEnvVar("HOME"))
    fprintf(port "V\t%L\tt\t%L\n" "source_work_dir" getWorkingDir())
    crescWriteStorageReadback(port hsdb)
  )
)
procedure(crescWriteNativeHistoryPaths(path history)
  let((port name result rdb location)
    name = axlGetHistoryName(history)
    rdb = nil location = nil
    when(isCallable('axlGetHistoryResults)
      result = errset(axlGetHistoryResults(history) t)
      when(result && stringp(car(result)) rdb = car(result))
    )
    when(isCallable('axlGetHistoryResultsDir)
      result = errset(axlGetHistoryResultsDir(history) t)
      when(result && stringp(car(result)) location = car(result))
    )
    port = outfile(path)
    unless(port error("Crescendo native history path readback cannot be created.\n"))
    fprintf(port "CRESCENDO_NATIVE_HISTORY_STORAGE_1\nH\t%L\t%L\t%L\n" name rdb location)
    close(port)
  )
)


procedure(crescMaestroWriteContext(session context @optional sourceWindow)
  let((setup lib cell view viewObj port outcome result history selected
       entry name variable libObj args handle windowNum modified policyTests)
    setup = axlGetMainSetupDB(session)
    unless(setup error("Crescendo: this ADE window has no setup database.\n"))
    lib = axlGetSessionLibName(session)
    cell = axlGetSessionCellName(session)
    view = axlGetSessionViewName(session)
    viewObj = ddGetObj(lib cell view)
    unless(viewObj error("Crescendo: cannot locate the invoking ADE view.\n"))
    selected = nil
    when(isCallable('axlGetCurrentHistory) && isCallable('axlGetHistoryName)
      result = errset(axlGetCurrentHistory(session) t)
      when(result
        history = car(result)
        when(history && history != 0
          result = errset(axlGetHistoryName(history) t)
          when(result && stringp(car(result)) selected = car(result))
        )
      )
    )
    windowNum = nil
    when(sourceWindow
      result = errset(sourceWindow->windowNum t)
      when(result && numberp(car(result)) windowNum = car(result))
    )
    ; A missing/failing API must not turn an unknown state into "saved".
    modified = "unknown"
    when(isCallable('maeIsSetupModified)
      result = errset(maeIsSetupModified(?session session) t)
      when(result && member(car(result) list(t nil)) modified = car(result))
    )
    port = outfile(context)
    unless(port error("Crescendo: cannot write a private launch context.\n"))
    outcome = errset(
      progn(
        fprintf(port "CRESCENDO_MAESTRO_CONTEXT_1\n")
        fprintf(port "C\t%L\t%L\t%L\t%L\t%L\t%L\t%L\n"
          lib cell view getWorkingDir() viewObj->readPath
          crescMaestroCurrentCdsLib() prependInstallPath("bin/ocean"))
        fprintf(port "S\t%L\n" selected)
        fprintf(port "W\t%L\n" windowNum)
        fprintf(port "M\t%L\n" modified)
        fprintf(port "R\t%L\n" getShellEnvVar("LD_LIBRARY_PATH"))
        foreach(variable '("HOME" "PDK" "CRESCENDO_SPECTRE_MODEL_DIR" "CRESCENDO_ALPS_MODEL_DIR" "PATH" "CDS_LIC_FILE" "LM_LICENSE_FILE" "LD_PRELOAD"
                            "PYTHONHOME" "PYTHONPATH" "TCL_LIBRARY" "TK_LIBRARY" "QT_PLUGIN_PATH")
          fprintf(port "E\t%L\t%L\n" variable getShellEnvVar(variable))
        )
        when(isCallable('axlGetHistory) && isCallable('axlGetHistoryEntry)
             && isCallable('axlGetHistoryResultsDir) && isCallable('axlGetHistoryResults)
          result = errset(axlGetHistory(setup) t)
          when(result
            ; Native order is meaningful; do not sort by names or file mtime.
            foreach(name cadr(car(result))
              result = errset(axlGetHistoryEntry(setup name) t)
              when(result && car(result)
                entry = car(result)
                result = errset(list(axlGetHistoryResultsDir(entry)
                                    axlGetHistoryResults(entry)) t)
                when(result
                  fprintf(port "H\t%L\t%L\t%L\n" name
                    car(car(result)) cadr(car(result)))
                )
              )
            )
          )
        )
        when(isCallable('ddGetLibList)
          result = errset(ddGetLibList() t)
          when(result
            foreach(libObj car(result)
              result = errset(list(libObj->name libObj->readPath) t)
              when(result && stringp(car(car(result))) && stringp(cadr(car(result)))
                fprintf(port "L\t%L\t%L\n" car(car(result)) cadr(car(result)))
              )
            )
          )
        )
        when(isCallable('axlGetTests) && isCallable('axlGetTest)
             && isCallable('axlGetTestToolArgs)
          result = errset(axlGetTests(setup) t)
          when(result
            foreach(name cadr(car(result))
              result = errset(axlGetTest(setup name) t)
              when(result && car(result)
                handle = car(result)
                result = errset(axlGetTestToolArgs(handle) t)
                when(result
                  args = car(result)
                  fprintf(port "T\t%L\t%L\t%L\t%L\t%L\n" name
                    cadr(assoc("lib" args)) cadr(assoc("cell" args))
                    cadr(assoc("view" args)) cadr(assoc("sim" args)))
                )
              )
            )
          )
        )
        policyTests = nil
        when(isCallable('axlGetTests)
          result = errset(axlGetTests(setup) nil)
          when(result policyTests = cadr(car(result)))
        )
        crescWriteJobPolicyRecords(port session policyTests)
        crescWriteSaveOptionsRecords(port setup)
        t
      ) t)
    close(port)
    unless(outcome && car(outcome)
      error("Crescendo: source context export failed; see CIW diagnostics.\n")
    )
    context
  )
)

procedure(crescMaestroOpenCurrent()
  let((sourceWindow session context launchScript outcome process)
    sourceWindow = hiGetCurrentWindow()
    session = axlGetWindowSession(sourceWindow)
    unless(session error("Open Crescendo from the source ADE window.\n"))
    unless(isFile(crescPluginLauncher)
      error("Crescendo: launcher is unavailable: %s\n" crescPluginLauncher)
    )
    ; Generate the name in the same directory where outfile creates it.
    context = makeTempFileName(strcat(getTempDir() "/crescendo_ctx_XXXXXX"))
    outcome = errset(
      progn(
        crescMaestroWriteContext(session context sourceWindow)
        ; Each click creates an independent UI bound to this window.
        launchScript = crescMaestroWriteLaunchScript(context)
        process = ipcBeginProcess(strcat("/bin/bash " launchScript)
          "" 'crescMaestroLaunchOutput 'crescMaestroLaunchOutput 'crescMaestroLaunchExit)
        unless(process error("Crescendo: cannot start the optimizer UI.\n"))
        crescMaestroLaunchJobs[process] = list(launchScript context)
        t
      ) t)
    unless(outcome && car(outcome)
      when(launchScript && isFile(launchScript) deleteFile(launchScript))
      when(isFile(context) deleteFile(context))
      error("Crescendo: launch failed; see CIW diagnostics.\n")
    )
    printf("Crescendo launch requested for %s/%s/%s\n"
      axlGetSessionLibName(session) axlGetSessionCellName(session)
      axlGetSessionViewName(session))
    process
  )
)

procedure(crescMaestroMenuItemFactory()
  hiCreateMenuItem(
    ?name 'crescMaestroOpenItem ?itemText "Open Crescendo Optimizer"
    ?callback "crescMaestroOpenCurrent()"
  )
)

; Preserve every other plugin factory and avoid duplicate registration on reload.
unless(boundp('maestroPluginItemFactories) maestroPluginItemFactories = nil)
unless(member('crescMaestroMenuItemFactory maestroPluginItemFactories)
  maestroPluginItemFactories = append(maestroPluginItemFactories list('crescMaestroMenuItemFactory))
)

procedure(crescMaestroMenuTrigger(_args)
  if(boundp('maestroPluginMenuExtensionVersion)
     && numberp(maestroPluginMenuExtensionVersion)
     && maestroPluginMenuExtensionVersion >= 1 then
    nil
  else
    list(hiCreatePulldownMenu(
      'crescMaestroMenu "Crescendo" list(crescMaestroMenuItemFactory())
    ))
  )
)

unless(boundp('crescMaestroMenusRegistered) && crescMaestroMenusRegistered
  deRegUserTriggers("explorer" nil 'crescMaestroMenuTrigger)
  deRegUserTriggers("maestro" nil 'crescMaestroMenuTrigger)
  deRegUserTriggers("adexl" nil 'crescMaestroMenuTrigger)
  crescMaestroMenusRegistered = t
)
