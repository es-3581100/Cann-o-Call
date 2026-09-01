#!/usr/bin/env python3
import argparse, json, platform, subprocess
from pathlib import Path

def run(root, *args):
    return subprocess.check_output(args, cwd=root, text=True).strip()

p=argparse.ArgumentParser()
p.add_argument('--root', required=True)
p.add_argument('--out', required=True)
p.add_argument('--version', required=True)
p.add_argument('--go-sha', required=True)
p.add_argument('--rust-sha', required=True)
p.add_argument('--goos', required=True)
p.add_argument('--goarch', required=True)
p.add_argument('--source-date-epoch', required=True, type=int)
a=p.parse_args()
root=Path(a.root).resolve()
obj={
  'schema':'cann-o-call-buildinfo/v1',
  'project':'Cann-o-Call',
  'version':a.version,
  'source_head':run(root,'git','rev-parse','HEAD'),
  'source_tree':run(root,'git','rev-parse','HEAD^{tree}'),
  'source_commit_epoch':a.source_date_epoch,
  'go_version':run(root,'go','version'),
  'rustc_version':run(root,'rustc','--version'),
  'cargo_version':run(root,'cargo','--version'),
  'go_target':f'{a.goos}/{a.goarch}',
  'host':platform.machine(),
  'artifacts':{
     'cann-o-call':{'sha256':a.go_sha,'reproducible_pair_match':True},
     'msl-ledger-sidecar':{'sha256':a.rust_sha,'reproducible_pair_match':True},
  },
  'authority_note':'Go admission; Rust durable acknowledgement; Proto.Actor lifecycle; capabilities/observability are non-authority',
}
Path(a.out).write_text(json.dumps(obj,indent=2,sort_keys=True)+'\n')
