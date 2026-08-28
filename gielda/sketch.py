"""Szkic kwantylowy do mediany (Miara 3): histogram logarytmiczny w stylu DDSketch.

Wlasnosci wymagane przez rubryke: ograniczony blad wzgledny (alpha), stala
pamiec, scalanie przez sumowanie licznikow, serializacja do zwyklego JSON-a.
"""
import json
import math


class LogHistogram:
    """Kubelek dla x > 0 to ceil(log_gamma x), gamma = (1+alpha)/(1-alpha).

    Kazdy kubelek pokrywa +-alpha swojej wartosci, wiec odpowiedz o kwantyl
    ma blad wzgledny <= alpha. Zera liczone osobno (log(0) nie istnieje),
    a zakres 0 zdarza sie naprawde: okno z jednym tickiem ma min = max.
    """

    def __init__(self, alpha=0.01, buckets=None, zero_count=0):
        self.alpha = alpha
        self.gamma = (1 + alpha) / (1 - alpha)
        self.buckets = buckets if buckets is not None else {}
        self.zero_count = zero_count

    def add(self, x):
        if x <= 0:
            self.zero_count += 1
        else:
            i = math.ceil(math.log(x, self.gamma))
            self.buckets[i] = self.buckets.get(i, 0) + 1

    def merge(self, other):
        """Scala drugi szkic (suma licznikow) - to wlasnosc umozliwiajaca laczenie dob."""
        if other.alpha != self.alpha:
            raise ValueError(
                f"nie mozna scalic szkicow o roznym alpha: {self.alpha} != {other.alpha}")
        self.zero_count += other.zero_count
        for i, c in other.buckets.items():
            self.buckets[i] = self.buckets.get(i, 0) + c

    def quantile(self, q):
        """Zwraca srodek kubelka, w ktorym wypada zadana ranga (0.5 = mediana)."""
        total = self.zero_count + sum(self.buckets.values())
        if total == 0:
            return 0.0
        rank = q * (total - 1)
        if rank < self.zero_count:
            return 0.0
        cum = self.zero_count
        for i in sorted(self.buckets):
            cum += self.buckets[i]
            if cum > rank:
                return 2 * self.gamma ** i / (self.gamma + 1)
        return 2 * self.gamma ** max(self.buckets) / (self.gamma + 1)

    def to_json(self):
        return json.dumps({"alpha": self.alpha, "zero_count": self.zero_count,
                           "buckets": self.buckets})

    @classmethod
    def from_json(cls, s):
        d = json.loads(s)
        # klucze kubelkow wracaja z JSON-a jako stringi
        return cls(alpha=d["alpha"],
                   buckets={int(k): v for k, v in d["buckets"].items()},
                   zero_count=d["zero_count"])
