import { useEffect, useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { getOwnedLinks, startGithubLink, unlinkOwnedAccount, type OwnedLink } from "../../api/client";
import { Button, Card, CliHint, Modal } from "../../primitives";
import { PARITY_TRACKING_ISSUE } from "../../primitives/parity";
import { C } from "../../tokens";

const LINKS_KEY = ["owned-account-links"] as const;
const RESULTS: Record<string, string> = {
  linked: "Your GitHub account is linked.",
  taken: "That GitHub account is already linked; unlink it from its current account first.",
  failed: "GitHub could not be linked; try again from this page.",
};

export function LinkedAccounts() {
  const queryClient = useQueryClient();
  const query = useQuery({ queryKey: LINKS_KEY, queryFn: getOwnedLinks, gcTime: 0 });
  const [result] = useState(() => new URLSearchParams(location.search).get("github"));
  const [confirming, setConfirming] = useState<OwnedLink | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const running = useRef(false);

  useEffect(() => {
    const url = new URL(location.href);
    if (url.searchParams.has("github")) {
      url.searchParams.delete("github");
      history.replaceState(history.state, "", url.pathname + url.search + url.hash);
    }
  }, []);

  async function run(send: () => Promise<void>) {
    if (running.current) return;
    running.current = true;
    setBusy(true);
    setError(null);
    try {
      await send();
    } catch {
      setError("Curie could not change your linked accounts. Try again in a minute.");
    } finally {
      running.current = false;
      setBusy(false);
    }
  }

  function unlink() {
    if (!confirming) return;
    const id = confirming.id;
    setConfirming(null);
    void run(async () => {
      await unlinkOwnedAccount(id);
      await queryClient.invalidateQueries({ queryKey: LINKS_KEY });
    });
  }

  return (
    <Card>
      <div style={{ fontSize: 15, fontWeight: 500, marginBottom: 10 }}>Linked accounts</div>
      {result && RESULTS[result] ? (
        <div role="status" style={{ fontSize: 13, color: C.text2, marginBottom: 12 }}>{RESULTS[result]}</div>
      ) : null}
      {query.isPending ? <div style={{ color: C.muted }}>Loading linked accounts...</div> : null}
      {query.isError ? (
        <div role="alert" style={{ fontSize: 13, color: C.destructive }}>Curie could not load your linked accounts.</div>
      ) : null}
      {query.data?.links.length === 0 ? <div style={{ fontSize: 13, color: C.muted }}>No accounts linked yet.</div> : null}
      {query.data?.links.map((link) => (
        <div key={link.id} data-testid="linked-account" style={{ display: "flex", alignItems: "center", gap: 12, padding: "10px 0", borderBottom: "1px solid " + C.border }}>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 13, color: C.text }}>{link.provider === "github" ? "GitHub" : "Slack"}: {link.display_label}</div>
            <div style={{ fontSize: 12, color: C.muted, marginTop: 4 }}>Linked {new Date(link.created_at).toLocaleDateString()}</div>
          </div>
          <Button label="Unlink" variant="danger" disabled={busy} onClick={() => setConfirming(link)} />
          <CliHint noCliEquivalent={PARITY_TRACKING_ISSUE} actionIds={["account-unlink"]} />
        </div>
      ))}
      {query.data?.github_link_available ? (
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginTop: 14 }}>
          <Button label="Link GitHub" disabled={busy} onClick={() => void run(async () => {
            const started = await startGithubLink();
            location.assign(started.authorize_url);
          })} />
          <CliHint noCliEquivalent={PARITY_TRACKING_ISSUE} actionIds={["github-account-link"]} />
        </div>
      ) : null}
      {error ? <div role="alert" style={{ fontSize: 13, color: C.destructive, marginTop: 12 }}>{error}</div> : null}
      {confirming ? (
        <Modal onClose={() => setConfirming(null)}>
          <div style={{ background: C.card, border: "1px solid " + C.borderStrong, borderRadius: 10, padding: 22, width: 400, maxWidth: "100%" }}>
            <div style={{ fontSize: 16, marginBottom: 8 }}>Unlink {confirming.display_label}?</div>
            <div style={{ fontSize: 13, color: C.text2, lineHeight: 1.5, marginBottom: 16 }}>
              This account will no longer start new runs on your Claude plan. Runs already admitted keep their owner.
            </div>
            <div style={{ display: "flex", justifyContent: "flex-end", gap: 8 }}>
              <Button label="Cancel" onClick={() => setConfirming(null)} />
              <Button label="Unlink account" variant="danger" onClick={unlink} />
            </div>
          </div>
        </Modal>
      ) : null}
    </Card>
  );
}
