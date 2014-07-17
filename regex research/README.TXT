Yet another attempt at parsing through logs trying to build customized regex.

* Open and advance fileObject position for skill logs
* Using a time stamp from skill log fetch a group of lines with equal stamps.
* Record a fileObject.tell() position as of the last equivalent time stamp in skill log.
* Open and advance fileObject position for event logs.
* Iter through event looking for a =< time stamp.
* With each iteration add the line to a LIFO stack, ''list.insert(0,'str').
* For the first NOT =< line record a fileObject.tell() position in event. Use another stack as file pointer
    will have advanced one line in order to see its gone too far.
* If  equal time stamps are found check if the event lines have a action completion message, use regex.
* Iter back through the LIFO stack looking for the action's start event line.

- Some skills do not have direct correlation between action completion and skill gain: repair, level.
- Sometimes there is a small 1 second timing difference skill logs and the completion time in event.