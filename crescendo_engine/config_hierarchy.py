"""Read-only occurrence binding with public Cadence HDB and PCDB APIs.

The returned SKILL is loaded into the same owned process as the normal OA
probe. It caches immutable PCDB master flags, never occurrence bindings.
"""


def is_schematic_implementation(view_name, view_type):
    """Only source schematic views expose an editable device hierarchy.

    Extracted OA implementations can also report the schematic database type;
    their names and exact HDB bindings must remain opaque to this scanner.
    """
    return (isinstance(view_name, str) and view_name.startswith('schematic')
            and view_type == 'schematic')


def config_probe_skill():
    return r'''
crescConfigStack = nil
crescConfigPcdbSeen = makeTable('crescConfigPcdbSeen 'crescNotSeen)
crescConfigViewTypes = makeTable('crescConfigViewTypes 'crescNotSeen)
procedure(crescConfigIsSchematic(viewName viewType)
  stringp(viewName) && rexMatchp("^schematic" viewName) && viewType == "schematic"
)
procedure(crescConfigFailure(kind path detail)
  fprintf(crescOut "%s\t%L\t%L\n" kind path detail)
)
procedure(crescConfigPcdb(libName cellName viewName path)
  prog((key cached pcId masters header insts pcInst info index count ignored)
    key = list(libName cellName viewName)
    cached = crescConfigPcdbSeen[key]
    unless(cached == 'crescNotSeen return(cached))
    pcId = pcdbOpen(libName cellName viewName "r" "CDBA")
    unless(pcId
      crescConfigFailure("CONFIG_PCDB_ERROR" path "read-only parent/child database could not be opened")
      return(nil)
    )
    index = makeTable('crescConfigPcdbInstances nil)
    count = 0
    foreach(ignored list(nil t)
    masters = if(ignored then pcdbGetIgnoredInstMasterGen(pcId) else pcdbGetInstMasterGen(pcId))
    header = pcdbNextInstMaster(masters)
    while(header
      info = list(pcdbInstMasterLib(header) pcdbInstMasterCell(header)
        pcdbInstMasterView(header) pcdbInstMasterLibFixed(header)
        pcdbInstMasterCellFixed(header) pcdbInstMasterViewFixed(header) ignored)
      insts = pcdbGetInstGen(header)
      pcInst = pcdbNextInst(insts)
      while(pcInst
        index[pcdbInstName(pcInst)] = info
        count = count + 1
        pcInst = pcdbNextInst(insts)
      )
      header = pcdbNextInstMaster(masters)
    )
    )
    pcdbClose(pcId)
    cached = list(index count)
    crescConfigPcdbSeen[key] = cached
    return(cached)
  )
)
procedure(crescConfigOpen(libName cellName viewName path)
  prog((config)
    unless(isCallable('hdbOpen) && isCallable('hdbHasIterInstBitRules)
      crescConfigFailure("CONFIG_API_ERROR" path "required public HDB API is unavailable")
      return(nil)
    )
    config = hdbOpen(libName cellName viewName "r" "CDBA")
    unless(config
      crescConfigFailure("CONFIG_OPEN_ERROR" path "config could not be opened read-only")
      return(nil)
    )
    fprintf(crescOut "CONFIG_DEP\t%L\t%L\t%L\n" libName cellName viewName)
    when(hdbHasIterInstBitRules(config)
      crescConfigFailure("CONFIG_BIT_RULES_UNSUPPORTED" path "iterated-instance bit-specific rules require separate per-bit traversal")
      hdbClose(config)
      return(nil)
    )
    return(config)
  )
)
procedure(crescConfigIgnoreReason(inst)
  let((master)
    master = inst~>master
    ; PCDB "ignored" means not a sub-design, not ignored by the netlister.
    ; Read the explicit OA action without obtaining any instance CDF. Native
    ; schematic port/ground markers are metadata even without an nlAction.
    cond(
      (inst~>nlAction == "ignore" "instance nlAction=ignore")
      (master~>nlAction == "ignore" "master nlAction=ignore")
      ((master~>libName == "basic" && member(master~>cellName list("ipin" "opin" "iopin"))) "schematic port marker")
      ((master~>libName == "analogLib" && master~>cellName == "gnd") "schematic ground marker")
    )
  )
)
procedure(crescConfigIsIgnored(inst)
  if(crescConfigIgnoreReason(inst) then t else nil)
)
procedure(crescConfigViewType(libName cellName viewName path)
  prog((key cached masterFile readable streamResult typeResult)
    key = list(libName cellName viewName)
    cached = crescConfigViewTypes[key]
    unless(cached == 'crescNotSeen return(cached))
    masterFile = ddGetObj(libName cellName viewName "*")
    unless(masterFile
      crescConfigFailure("CONFIG_VIEW_MISSING" path "resolved implementation has no master file")
      return(nil)
    )
    streamResult = errset(infile(masterFile~>readPath) t)
    unless(streamResult && car(streamResult)
      crescConfigFailure("CONFIG_VIEW_UNREADABLE" path "resolved implementation master file is not readable")
      return(nil)
    )
    close(car(streamResult))
    typeResult = errset(ddMapGetFileViewType(masterFile) t)
    unless(typeResult
      crescConfigFailure("CONFIG_API_ERROR" path "resolved implementation view type lookup failed")
      return(nil)
    )
    cached = if(car(typeResult) then car(typeResult) else "unknown")
    crescConfigViewTypes[key] = cached
    return(cached)
  )
)
procedure(crescProbeConfig(libName cellName viewName path depth)
  prog((identity config topLib topCell topView vector)
    identity = list(libName cellName viewName)
    when(member(identity crescConfigStack)
      crescConfigFailure("CONFIG_CYCLE" path identity)
      return(nil)
    )
    config = crescConfigOpen(libName cellName viewName path)
    unless(config return(nil))
    topLib = hdbGetTopLibName(config)
    topCell = hdbGetTopCellName(config)
    topView = hdbGetTopViewName(config)
    unless(topLib && topCell && topView && topLib != "" && topCell != "" && topView != ""
      crescConfigFailure("CONFIG_TOP_ERROR" path "config has no complete top identity")
      hdbClose(config)
      return(nil)
    )
    when(path == "/"
      fprintf(crescOut "RESOLVED_TOP\t%L\t%L\t%L\n" topLib topCell topView)
    )
    vector = hdbCreatePathVector(config)
    unless(vector
      crescConfigFailure("CONFIG_API_ERROR" path "HDB path vector could not be created")
      hdbClose(config)
      return(nil)
    )
    crescConfigStack = cons(identity crescConfigStack)
    crescProbeCell(topLib topCell topView path depth
      list(vector libName cellName viewName path) nil)
    crescConfigStack = cdr(crescConfigStack)
    hdbDestroyPathVector(vector)
    hdbClose(config)
    return(t)
  )
)
procedure(crescConfigBind(context pcdb inst ownerLib ownerCell ownerView parentPath instPath)
  prog((flags master vector binding boundLib boundCell boundView resolvedLib resolvedCell
        resolvedView stop config topLib topCell localPath viewType ignored traversable symbolMasked)
    master = inst~>master
    flags = car(pcdb)[inst~>name]
    unless(flags && car(flags) == master~>libName &&
      cadr(flags) == master~>cellName && nth(2 flags) == master~>viewName
      crescConfigFailure("CONFIG_PCDB_ERROR" instPath
        sprintf(nil "PCDB and actual OA instance identities disagree: PCDB=%L OA=%L nlAction=%L; save/check the source schematic and inspect this occurrence"
          flags list(master~>libName master~>cellName master~>viewName) inst~>nlAction))
      return(nil)
    )
    vector = car(context)
    ignored = if(crescConfigIsIgnored(inst) then t else nil)
    binding = if(ignored then list(car(flags) cadr(flags) nth(2 flags) nil)
      else hdbBind(vector car(flags) cadr(flags) nth(2 flags) inst~>name
      nth(3 flags) nth(4 flags) nth(5 flags) t nil))
    unless(binding && car(binding) && cadr(binding) && nth(2 binding)
      crescConfigFailure("CONFIG_BINDING_ERROR" instPath "HDB did not resolve this occurrence")
      return(nil)
    )
    boundLib = car(binding)
    boundCell = cadr(binding)
    boundView = nth(2 binding)
    when(boundLib == "" || boundCell == "" || boundView == ""
      crescConfigFailure("CONFIG_OPEN_BINDING_UNSUPPORTED" instPath "binding to open has no optimizable implementation")
      return(nil)
    )
    resolvedLib = boundLib
    resolvedCell = boundCell
    resolvedView = boundView
    unless(ignored hdbPushCell(vector inst~>name boundLib boundCell boundView))
    stop = if(ignored then t else hdbIsAtStopPoint(vector))
    when(!stop && hdbIsConfig(boundLib boundCell boundView)
      config = crescConfigOpen(boundLib boundCell boundView instPath)
      unless(config hdbPopCell(vector) return(nil))
      topLib = hdbGetTopLibName(config)
      topCell = hdbGetTopCellName(config)
      resolvedLib = topLib
      resolvedCell = topCell
      resolvedView = hdbGetTopViewName(config)
      hdbClose(config)
      unless(topLib && topCell && resolvedView &&
        (master~>libName == "*" || topLib == master~>libName) &&
        (master~>cellName == "*" || topCell == master~>cellName)
        crescConfigFailure("CONFIG_TOP_MISMATCH" instPath "nested config top differs from the original instance master")
        hdbPopCell(vector)
        return(nil)
      )
    )
    localPath = if(nth(4 context) == "/" then instPath
      else substring(instPath strlen(nth(4 context))+1))
    viewType = if(ignored then nil else crescConfigViewType(resolvedLib resolvedCell resolvedView instPath))
    unless(ignored || viewType
      hdbPopCell(vector)
      return(nil)
    )
    ; Netlister stop and scanner recursion are separate: HDL, extracted,
    ; simulator and symbol implementations never masquerade as schematics.
    traversable = !ignored && !stop && crescConfigIsSchematic(resolvedView viewType)
    symbolMasked = !ignored && resolvedView == "symbol"
    fprintf(crescOut "CONFIG_BINDING\t%L\t%L\t%L\t%L\t%L\t%L"
      instPath inst~>name parentPath master~>libName master~>cellName master~>viewName)
    fprintf(crescOut "\t%L\t%L\t%L\t%L\t%L\t%L\t%L\t%L\t%L\t%L"
      ownerLib ownerCell ownerView boundLib boundCell boundView
      resolvedLib resolvedCell resolvedView stop)
    fprintf(crescOut "\t%L\t%L\t%L\t%L\t%L\t%L\t%L\t%L\t%L\t%L\t%L\t%L\t%L\n"
      nth(1 context) nth(2 context) nth(3 context) nth(4 context) localPath
      nth(3 flags) nth(4 flags) nth(5 flags) viewType ignored nth(6 flags) traversable symbolMasked)
    return(list(boundLib boundCell boundView resolvedLib resolvedCell resolvedView stop nth(3 binding) ignored traversable symbolMasked viewType))
  )
)
'''
