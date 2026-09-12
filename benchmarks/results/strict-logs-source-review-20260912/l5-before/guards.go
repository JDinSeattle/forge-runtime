package main
import("encoding/json";"fmt";"os";"time")
func slNum(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case float64:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	}
	return -1
}

func acceptedSignal(signal map[string]any) bool {
 expected := "fixture-binary-sha"
 if slNum(signal["exit_code"]) != 0 || slNum(signal["pid"]) <= 0 || signal["exe_sha256"] != expected || signal["proc_start_ticks"] == "" { return false }
 sent,e := time.Parse(time.RFC3339Nano,fmt.Sprint(signal["signal_sent_at"])); if e!=nil{return false}
 exited,e := time.Parse(time.RFC3339Nano,fmt.Sprint(signal["exited_at"])); if e!=nil{return false}
 if !exited.After(sent) || exited.Sub(sent) > 8*time.Second { return false }; return true
}
func acceptedLease(oldUntil,claimed time.Time) bool {
 if claimed.Before(oldUntil) {return false}; return true
}
func main(){
 baseline := map[string]any{"exit_code":float64(0),"pid":float64(2000),"exe_sha256":"fixture-binary-sha","proc_start_ticks":"456789","signal_sent_at":"2026-09-12T01:00:00Z","exited_at":"2026-09-12T01:00:01Z"}
 results := map[string]any{"positive":acceptedSignal(baseline)}
 clone := func() map[string]any { m:=map[string]any{}; for k,v:=range baseline {m[k]=v}; return m }
 missing:=clone(); delete(missing,"proc_start_ticks"); results["missing_ticks_accepted"]=acceptedSignal(missing)
 pid:=clone(); pid["pid"]=float64(3000); results["replaced_positive_pid_accepted"]=acceptedSignal(pid)
 ticks:=clone(); ticks["proc_start_ticks"]="not-a-number"; results["nonnumeric_ticks_accepted"]=acceptedSignal(ticks)
 fractional:=clone(); fractional["pid"]=float64(2000.5); results["fractional_pid_accepted"]=acceptedSignal(fractional)
 old,_:=time.Parse(time.RFC3339Nano,"2000-01-01T00:00:00Z")
 claim,_:=time.Parse(time.RFC3339Nano,"2000-01-01T00:00:01Z")
 results["unbound_historical_summary_times_accepted"]=acceptedLease(old,claim)
 json.NewEncoder(os.Stdout).Encode(results)
}
