+++
title = "Middlewares"
description = "Add generic functionalities to your handlers in an inobtrusive way"
date = 2019-06-01T19:00:00+01:00
weight = -100
draft = false 
bref = "Add functionality to handlers"
toc = true
type = "docs"
+++

### Introduction

Middlewares wrap handlers with functionality that is important, but not relevant for the primary handler's logic. 
Examples include retrying the handler after an error was returned, or recovering from panic in the handler
and capturing the stacktrace.

Middlewares wrap the handler function like this:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/router.go" first_line_contains="// HandlerMiddleware" last_line_contains="type HandlerMiddleware" %}}
{{% /render-md %}}

{{% readfile file="/content/src-link/middleware-defs.md" markdown="true" %}}
