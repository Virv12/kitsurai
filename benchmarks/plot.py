import dataclasses
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.ticker import LogLocator, NullFormatter


@dataclasses.dataclass()
class Data:
    period: float = dataclasses.field()
    count: int = dataclasses.field()
    bad_count: int = dataclasses.field()
    avg: float = dataclasses.field()
    mdev: float = dataclasses.field()
    p0: float = dataclasses.field()
    p50: float = dataclasses.field()
    p90: float = dataclasses.field()
    p99: float = dataclasses.field()
    p999: float = dataclasses.field()
    p100: float = dataclasses.field()

    def __post_init__(self):
        self.period = float(self.period)
        self.count = int(self.count)
        self.bad_count = int(self.bad_count)
        self.avg = float(self.avg)
        self.mdev = float(self.mdev)
        self.p0 = float(self.p0)
        self.p50 = float(self.p50)
        self.p90 = float(self.p90)
        self.p99 = float(self.p99)
        self.p999 = float(self.p999)
        self.p100 = float(self.p100)


def main(file: Path):
    print(file)

    data = [Data(*line.split(',')) for line in file.read_text().splitlines()[1:]]
    print(f'{len(data)=}')

    periods = [d.period for d in data]
    freqs = [1000 / period for period in periods]
    p0s = [d.p0 for d in data]
    p50s = [d.p50 for d in data]
    p90s = [d.p90 for d in data]
    p99s = [d.p99 for d in data]
    p999s = [d.p999 for d in data]
    p100s = [d.p100 for d in data]

    plt.rcParams.update({"text.usetex": True})
    fig, ax = plt.subplots(figsize=(4, 4), dpi=1200)
    #fig.tight_layout()

    # set title
    ax.set_title(file.stem.split('-')[1].title())

    ax.set_xscale('log')
    ax.set_yscale('log')

    ax.set_xlabel('Request Frequency (Hz)')
    ax.set_ylabel('Response Latency (ms)')

    ax.plot(freqs, p0s, label='$P_0$')
    ax.plot(freqs, p50s, label='$P_{50}$')
    ax.plot(freqs, p90s, label='$P_{90}$')
    ax.plot(freqs, p99s, label='$P_{99}$')
    ax.plot(freqs, p999s, label='$P_{99.9}$')
    ax.plot(freqs, p100s, label='$P_{100}$')
    ax.legend()

    ax.grid()
    ax.grid(which='major', linestyle='-', linewidth=0.8)
    ax.grid(which='minor', linestyle=':', linewidth=0.3)

    #fig.show()
    fig.savefig(file.with_suffix(f'.svg'))
    fig.savefig(file.with_suffix(f'.pdf'))


if __name__ == "__main__":
    main(Path(__file__).parent / 'bench-get.csv')
    main(Path(__file__).parent / 'bench-set.csv')
