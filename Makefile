src = $(wildcard *.c)
obj = $(src:.c=.o)

test_assign1: $(obj)
	$(CC) -o $@ $^ 

.PHONY: clean
clean:
	rm -f $(obj) test_assign2
