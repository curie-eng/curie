#include <sys/prctl.h>

/* exec restores dumpable. Same-uid peers cannot read this process environ. */
static void __attribute__((constructor)) lock_dumpable(void)
{
    prctl(PR_SET_DUMPABLE, 0, 0, 0, 0);
}
