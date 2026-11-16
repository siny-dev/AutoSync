# AutoSync review candidate workflow

`build-review-candidate.ps1` creates an immutable, local candidate under
`AutoUpdate\review-candidates`. It calls the generic recursive
`AutoUpdate\tools\build-module-package.ps1`, then writes the matching resolver
layout (`modules\AutoSync\current.json` and `versions\<version>`). It never
calls `publish-module`, writes `outbox\modules\AutoSync\current.json`, or starts
an AutoSync server or worker.

The agreed editable baseline for S2 is the reconstructed tree at
`AutoUpdate\review-candidates\20261030\S2-AutoSync-0.1.24-editable-reconstruction`;
edit its `source` tree and retain `PROVENANCE.md`. It was reconstructed from the
immutable 0.1.24 package payload, not recovered from original editable history.
The main `AutoSync\src` checkout remains the separate 0.1.12 source and is not
the release input for this workflow.

The source tree must be an isolated reviewed copy containing
`src\__init__.py` and the requested semantic version. The builder refuses any
source tree overlapping the main AutoSync checkout, including the live
`AutoSync\src` at `.12`. It copies recursively while excluding backup files,
bytecode, cache folders, hidden metadata, and generated AutoUpdate manifests.
The current Workboard module contract is API major 1 with restart activation;
patch and minor versions can use the same wrapper. API major or activation
contract changes require coordinated Workboard integration work.

Example using the immutable `.24` payload as reviewed input:

```powershell
$sourceRoot = 'C:\AI-Work\제작중인 프로그램\AutoUpdate\review-candidates\20261029\S2-AutoSync-local-share-0.1.24\package\payload'
$candidateRoot = 'C:\AI-Work\제작중인 프로그램\AutoUpdate\review-candidates\20261030\S2-AutoSync-0.1.24-editable-reconstruction'
& 'C:\AI-Work\제작중인 프로그램\AutoSync\tools\build-review-candidate.ps1' `
  -SourceRoot $sourceRoot -Version '0.1.24' -CandidateRoot $candidateRoot
```

Run the version-aware, loopback-only tests in a reconstructed candidate with:

```powershell
python tools\run_version_aware_tests.py
```

The output is a review candidate, not an official source pointer or release.
