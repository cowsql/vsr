CC = cc
CXX = c++

.PHONY: check
check:
	$(CC) $(CPPFLAGS) $(CFLAGS) -std=c11 -Wall -Wextra -Werror -pedantic -Iinclude -fsyntax-only tests/header.c
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -std=c++11 -Wall -Wextra -Werror -pedantic -Iinclude -x c++ -fsyntax-only tests/header.c
