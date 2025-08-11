## Rust-Practice

原链接是 [https://github.com/rosedblabs/rust-practice](https://github.com/rosedblabs/rust-practice)

some tiny learning projects in Rust, awesome!

* mini-bitcask-rs
* expr-eval
* mvcc(multi version concurrency control)

***



## expr-eval2

[expr-eval2](./expr-eval2)提供了三种实现方式。



一开始，对着作者的源码，手敲。作者的写法是“递归下降解析器”，这个以前没有见过。



搞明白之后，自己做了个“双栈”，`direct_evaluation`，这个之前在学算法或者数据结构的时候写过。核心思路，

- 左到右遍历，数字进数据栈
- 遇到`(`，压栈；遇到`)`，就pop符号做计算，直到遇到`(`。
- 遇到符号就判断优先级，栈的优先级高，就pop符号，先计算。这里有个要注意的是`^`的“右结合”的问题，右边先算。栈顶符号优先级要严格大于遍历的运算符。否则，等于也可以，左边先算。
  - 举例9+2\*5，遇到\*，\*压栈，不会马上计算，符号压栈。
  - 举例9+1+2\*5，遇到第二个+，+不压栈，会马上计算9+1=10，然后数据压栈。



AI给我写了个`shunting_yard_evaluate`，中序转后序，然后再计算。
