%%{
machine base;

action observe_path_root { observe_path_root(); }
action observe_path_dot { observe_path_dot(); }
action observe_path_index { observe_path_index(); }

action mark_start {mark_start(fpc);}
action mark_end {mark_end(fpc);}

integer = digit+ >mark_start %mark_end;
name = alpha ( alnum | '_' )* ;
nameref = '#' ( alnum | '_' )+ ;
path_component = (name|nameref) >mark_start %mark_end;
path = space* path_component %observe_path_root
(
    ('.' path_component) %observe_path_dot
    | ('[' integer ']') %observe_path_index
)* space* ;
}%%