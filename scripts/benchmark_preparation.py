"""Developer-only synthetic frontend benchmark; no EDA processes are launched."""
import json
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tests.test_preparation_timing import frontend_observations

if __name__ == '__main__':
    print(json.dumps({'fixture': 'Synthetic native child: 10ms startup, 30ms capture, 10ms shutdown; real file freezing and symbolic proof',
                      'real_eda': False, 'modes': frontend_observations(0.01)}, indent=2, sort_keys=True))
