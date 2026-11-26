package cost

import (
	"errors"
	"math"
	"testing"
)

func TestRankFleetMultiProviderAndSorting(t *testing.T) {
	placements, err := RankFleet(0, "", 0)
	if err != nil {
		t.Fatalf("RankFleet failed: %v", err)
	}
	if len(placements) == 0 {
		t.Fatal("expected non-empty placements")
	}

	providers := map[string]bool{}
	regions := map[string]bool{}
	for i, p := range placements {
		providers[p.Pool.Provider] = true
		regions[p.Pool.Region] = true
		if p.EffectiveHourlyUSD <= 0 {
			t.Errorf("placement %d EffectiveHourlyUSD = %v, want > 0", i, p.EffectiveHourlyUSD)
		}
		if p.CompositeScore <= 0 || p.CompositeScore > 1 {
			t.Errorf("placement %d CompositeScore = %v, want in (0, 1]", i, p.CompositeScore)
		}
		if i > 0 {
			prev := placements[i-1]
			if prev.CompositeScore < p.CompositeScore {
				t.Fatalf("placements not sorted descending by CompositeScore at %d: %v < %v", i, prev.CompositeScore, p.CompositeScore)
			}
			if prev.CompositeScore == p.CompositeScore && prev.EffectiveHourlyUSD > p.EffectiveHourlyUSD {
				t.Fatalf("tie-break failed at %d: %v > %v", i, prev.EffectiveHourlyUSD, p.EffectiveHourlyUSD)
			}
		}
	}

	for _, wantProvider := range []string{"gcp", "aws", "oci"} {
		if !providers[wantProvider] {
			t.Errorf("missing provider %q in multi-provider fleet ranking", wantProvider)
		}
	}
	for _, wantRegion := range []string{"us-central1", "europe-west4", "us-east-1", "us-west-2", "us-phoenix-1", "uk-london-1"} {
		if !regions[wantRegion] {
			t.Errorf("missing region %q in multi-provider fleet ranking", wantRegion)
		}
	}
}

func TestRankFleetProviderFiltering(t *testing.T) {
	for _, provider := range []string{"oci", "gcp", "aws"} {
		t.Run(provider, func(t *testing.T) {
			placements, err := RankFleet(24, provider, 0)
			if err != nil {
				t.Fatalf("RankFleet(%s) failed: %v", provider, err)
			}
			if len(placements) == 0 {
				t.Fatalf("expected non-empty placements for provider %q", provider)
			}
			for _, p := range placements {
				if p.Pool.Provider != provider {
					t.Errorf("got provider %q, want %q", p.Pool.Provider, provider)
				}
				if p.Pool.VRAMGB < 24 {
					t.Errorf("got VRAM %d, want >= 24", p.Pool.VRAMGB)
				}
			}
		})
	}

	// Case-insensitive and whitespace-trimmed provider filter.
	placements, err := RankFleet(80, " OCI ", 0)
	if err != nil {
		t.Fatalf("RankFleet( OCI ) failed: %v", err)
	}
	if len(placements) == 0 {
		t.Fatal("expected non-empty placements for trimmed OCI filter")
	}
	for _, p := range placements {
		if p.Pool.Provider != "oci" || p.Pool.VRAMGB < 80 {
			t.Errorf("unexpected placement %+v", p.Pool)
		}
	}
}

func TestRankFleetAvailabilityThreshold(t *testing.T) {
	placements, err := RankFleet(0, "", 0.90)
	if err != nil {
		t.Fatalf("RankFleet failed: %v", err)
	}
	if len(placements) == 0 {
		t.Fatal("expected placements with availability >= 0.90")
	}
	for _, p := range placements {
		if p.Pool.AvailabilityScore < 0.90 {
			t.Errorf("got AvailabilityScore %v, want >= 0.90", p.Pool.AvailabilityScore)
		}
	}

	none, err := RankFleet(0, "", 0.99)
	if err != nil {
		t.Fatalf("RankFleet failed: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("expected 0 placements for availability >= 0.99, got %d", len(none))
	}
}

func TestRankFleetValidationErrors(t *testing.T) {
	cases := []struct {
		name            string
		minVRAM         int
		provider        string
		minAvailability float64
		wantErr         error
	}{
		{name: "negative vram", minVRAM: -1, provider: "", minAvailability: 0, wantErr: ErrInvalidVRAM},
		{name: "negative availability", minVRAM: 0, provider: "", minAvailability: -0.1, wantErr: ErrInvalidAvailability},
		{name: "availability above 1", minVRAM: 0, provider: "", minAvailability: 1.1, wantErr: ErrInvalidAvailability},
		{name: "nan availability", minVRAM: 0, provider: "", minAvailability: math.NaN(), wantErr: ErrInvalidAvailability},
		{name: "unknown provider", minVRAM: 0, provider: "azure", minAvailability: 0, wantErr: ErrUnknownProvider},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := RankFleet(tc.minVRAM, tc.provider, tc.minAvailability)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("RankFleet() error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}
