# Sourced by bash before a Bash tool command. Unsets platform credentials
# that the CLI parent still needs. Names only; this file holds no values.
unset ANTHROPIC_API_KEY ANTHROPIC_AUTH_TOKEN CLAUDE_CODE_OAUTH_TOKEN \
  ANTHROPIC_FOUNDRY_API_KEY ANTHROPIC_CUSTOM_HEADERS CURIE_CREDENTIALS
if [ -n "${CURIE_MODEL_ENV_KEY-}" ]; then
  case $CURIE_MODEL_ENV_KEY in
    \[*)
      ;;
    *)
      unset -- "$CURIE_MODEL_ENV_KEY"
      ;;
  esac
fi
while IFS= read -r name; do
  case $name in
    CURIE_*TOKEN*)
      unset -- "$name"
      ;;
  esac
done < <(compgen -e)
