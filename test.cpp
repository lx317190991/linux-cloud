#include <iostream>
#include <stdlib.h>

int RunScript(const char *const cmd, char *result, size_t len)
{
	if (cmd == NULL)
	{
		return -3;
	}
	int status;
	int ret = 0;
	if (result != NULL)
	{
		FILE  *fp = popen(cmd, "r");
		size_t read_len = 0;
		if (!fp)
		{
			pclose(fp);
			return -1;
		}
		read_len = fread(result, 1, len - 1, fp);
		if (ferror(fp))
		{
			pclose(fp);
			return -2;
		}
		else
		{
			result[read_len] = '\0';
		}
		status = pclose(fp);
	}
	else
	{
		status = 0;
	}
	if (WIFEXITED(status))
	{
		ret = WEXITSTATUS(status);
	}
	else
	{
		ret = -4;
	}
	return ret;
}

int main() {
	int ret = RunScript("/opt/opthb/bin/bolparam -set process.ProgramDef.app1.Priority=5", NULL, 0);
	std::cout << ret << std::endl;
}

