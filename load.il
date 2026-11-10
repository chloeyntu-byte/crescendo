; Load this file from the extracted source folder, for example:
;   load("./crescendo/load.il")
; Resource paths come from this file, independently of the invoking workspace.
let((entryFile entryParts sourceRoot menu launcher resource
     hadLauncher previousLauncher outcome)
  entryFile = get_filename(piport)
  unless(stringp(entryFile) && entryFile != ""
    error("Crescendo: load the extracted load.il file; its source path is unavailable.\n")
  )
  unless(substring(entryFile 1 1) == "/"
    entryFile = strcat(getWorkingDir() "/" entryFile)
  )
  ; Avoid shell/environment expansion of literal characters in folder names.
  entryParts = parseString(entryFile "/")
  sourceRoot = strcat("/" buildString(reverse(cdr(reverse(entryParts))) "/"))
  menu = strcat(sourceRoot "/cadence/crescendo_maestro_menu.il")
  launcher = strcat(sourceRoot "/cadence/crescendo_launch")
  foreach(resource list(menu launcher strcat(sourceRoot "/crescendo/app.py") strcat(sourceRoot "/crescendo_engine/__main__.py"))
    unless(isFile(resource)
      error("Crescendo: the extracted folder is incomplete; required resource is missing: %s\n" resource)
    )
  )
  hadLauncher = boundp('crescPluginLauncher)
  when(hadLauncher previousLauncher = crescPluginLauncher)
  ; Always select this copy, including reloads after another installation.
  crescPluginLauncher = launcher
  outcome = errset(load(menu) t)
  unless(outcome
    crescPluginLauncher = if(hadLauncher then previousLauncher else nil)
    error("Crescendo: source menu loading failed; see CIW diagnostics.\n")
  )
  printf("Crescendo source plugin loaded from %s\n" sourceRoot)
)
