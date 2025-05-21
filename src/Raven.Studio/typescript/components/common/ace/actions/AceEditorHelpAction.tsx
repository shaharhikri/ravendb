import Button from "react-bootstrap/Button";
import useDialog, { DialogOptions } from "../../Dialog";
import { Icon } from "../../Icon";
import { ReactNode } from "react";

interface AceEditorHelpActionProps extends Omit<DialogOptions, "title"> {
    title?: ReactNode;
    tooltipTitle?: string;
}

export default function AceEditorHelpAction({
    title = "Syntax help",
    tooltipTitle = "Syntax help",
    actionColor = "info",
    modalSize = "lg",
    ...rest
}: AceEditorHelpActionProps) {
    const dialog = useDialog();

    const handleOpen = () => {
        dialog({
            title,
            actionColor,
            modalSize,
            ...rest,
        });
    };

    return (
        <Button variant="link" onClick={handleOpen} className="p-0 text-reset" size="sm" title={tooltipTitle}>
            <Icon icon="help" margin="m-0" />
        </Button>
    );
}
