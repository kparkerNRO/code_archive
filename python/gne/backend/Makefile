export SERVICE_NAME = agent_companion

include ../../dev/common.mk

help:
	@echo "Agent Companion targets:"
	@grep -E '^[a-zA-Z0-9_.-]+:.*?###' $(MAKEFILE_LIST) | \
		sed -e 's/:.*###/:/' -e 's/###//g' | \
		sort | \
		awk 'BEGIN {FS=":"} {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Common targets:"
	@grep -E '^[a-zA-Z0-9_.-]+:.*?##' ../../dev/common.mk | \
		sed -e 's/:.*##/:/' -e 's/##//g' | \
		sort | \
		awk 'BEGIN {FS=":"} {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
	@echo ""