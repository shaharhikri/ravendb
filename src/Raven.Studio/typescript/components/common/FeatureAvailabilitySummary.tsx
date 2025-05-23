import classNames from "classnames";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppSelector } from "components/store";
import { uniqueId } from "lodash";
import { ReactNode } from "react";
import Table from "react-bootstrap/Table";
import Button from "react-bootstrap/Button";
import IconName from "typings/server/icons";
import { licenseSelectors } from "./shell/licenseSlice";
import { Icon } from "./Icon";
import "./FeatureAvailabilitySummary.scss";
import RichAlert from "components/common/RichAlert";
import appUrl from "common/appUrl";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import PopoverWithHoverWrapper from "./PopoverWithHoverWrapper";
import useBoolean from "components/hooks/useBoolean";
import Modal from "components/common/Modal";

export type AvailabilityValue = boolean | number | string;

export interface FeatureAvailabilityValueData {
    value: AvailabilityValue;
    overwrittenValue?: AvailabilityValue;
}

export interface FeatureAvailabilityData {
    featureName?: string;
    featureIcon?: IconName;
    helperInfo?: ReactNode;
    community: FeatureAvailabilityValueData;
    professional?: FeatureAvailabilityValueData;
    enterprise: FeatureAvailabilityValueData;
    enterpriseAi?: FeatureAvailabilityValueData;
}

interface FeatureAvailabilitySummaryProps {
    data: FeatureAvailabilityData[];
}

export function FeatureAvailabilitySummary(props: FeatureAvailabilitySummaryProps) {
    const { data } = props;

    const currentLicense = useAppSelector(licenseSelectors.licenseType);
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));
    const isIsv = useAppSelector(licenseSelectors.statusValue("IsIsv"));

    const buyLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    if (!currentLicense) {
        return null;
    }

    const licenseTypes = isCloud ? ["Free", "Production"] : ["Community", "Professional", "Enterprise", "EnterpriseAi"];

    if (currentLicense === "Developer") {
        licenseTypes.push("Developer");
    }

    const getLicenseTypeTitle = (licenseType: string) => {
        if (licenseType === "Developer") {
            return "Dev";
        }

        if (licenseType === "EnterpriseAi") {
            return "RavenDB AI";
        }

        return licenseType;
    };

    return (
        <>
            {currentLicense === "None" && (
                <RichAlert variant="danger" className="mb-4">
                    No license detected.
                    <br />
                    You are using RavenDB using <strong>AGPL v3 License</strong>.
                    <br />
                    Community license feature restrictions are applied.
                </RichAlert>
            )}
            <div className="feature-availability-table">
                <Table>
                    <thead>
                        <tr>
                            <th className="p-0"></th>
                            {licenseTypes.map((licenseType) => {
                                if (isIsv && licenseType === "Community") {
                                    return (
                                        <th
                                            key="Essential"
                                            className={classNames("community", {
                                                "current bg-faded-primary": currentLicense === "Essential",
                                            })}
                                        >
                                            <Icon icon="circle-filled" className="license-dot" /> Essential
                                        </th>
                                    );
                                }
                                return (
                                    <th
                                        key={licenseType}
                                        className={classNames("position-relative", licenseType.toLowerCase(), {
                                            "current bg-faded-primary":
                                                currentLicense === licenseType ||
                                                (currentLicense === "None" && licenseType === "Community") ||
                                                (currentLicense === "Community" && licenseType === "Free") ||
                                                (currentLicense === "Enterprise" && licenseType === "Production") ||
                                                (currentLicense === "EnterpriseAi" && licenseType === "Enterprise"),
                                        })}
                                    >
                                        <Icon icon="circle-filled" className="license-dot" />
                                        {getLicenseTypeTitle(licenseType)}
                                        {licenseType === "Developer" && (
                                            <PopoverWithHoverWrapper
                                                message={
                                                    <div className="text-center">
                                                        <div>
                                                            Developer license enables{" "}
                                                            <strong>Enterprise License features</strong> but is{" "}
                                                            <strong>not applicable for commercial use</strong>.
                                                        </div>

                                                        <Button
                                                            variant="link"
                                                            size="sm"
                                                            href="https://ravendb.net/l/FLDLO4#developer"
                                                            target="_blank"
                                                        >
                                                            See details <Icon icon="newtab" margin="ms-1" />
                                                        </Button>
                                                    </div>
                                                }
                                            >
                                                <div className="corner-info">
                                                    <Icon icon="info" margin="m-0" />
                                                </div>
                                            </PopoverWithHoverWrapper>
                                        )}
                                    </th>
                                );
                            })}
                        </tr>
                    </thead>
                    <tbody>
                        {data.map((data, idx) => (
                            <tr key={idx} className="feature-row">
                                <th className="p-0">
                                    {data.featureName && (
                                        <div className="p-2">
                                            {data.featureIcon && <Icon icon={data.featureIcon} />}
                                            {data.featureName}
                                            {data.helperInfo && (
                                                <OverlayTrigger
                                                    placement="top"
                                                    overlay={<Tooltip id={data.featureName}>{data.helperInfo}</Tooltip>}
                                                >
                                                    <div className="d-inline-block">
                                                        <Icon icon="info" color="info" className="ms-1" />
                                                    </div>
                                                </OverlayTrigger>
                                            )}
                                        </div>
                                    )}
                                </th>
                                <td
                                    className={classNames("community", {
                                        "current bg-faded-primary":
                                            currentLicense === "Community" ||
                                            currentLicense === "Essential" ||
                                            currentLicense === "None",
                                    })}
                                >
                                    {formatAvailabilityValue(data.community)}
                                </td>
                                {!isCloud && (
                                    <td
                                        className={classNames("professional", {
                                            "current bg-faded-primary": currentLicense === "Professional",
                                        })}
                                    >
                                        {formatAvailabilityValue(data.professional)}
                                    </td>
                                )}
                                <td
                                    className={classNames("enterprise", {
                                        "current bg-faded-primary": currentLicense === "Enterprise",
                                    })}
                                >
                                    {formatAvailabilityValue(data.enterprise, isCloud)}
                                </td>
                                <td
                                    className={classNames("enterprise-ai", {
                                        "current bg-faded-primary": currentLicense === "EnterpriseAi",
                                    })}
                                >
                                    {formatAvailabilityValue(data.enterpriseAi, isCloud)}
                                </td>
                                {currentLicense === "Developer" && (
                                    <td
                                        className={classNames("developer", {
                                            "current bg-faded-primary": currentLicense === "Developer",
                                        })}
                                    >
                                        {formatAvailabilityValue(data.enterprise, isCloud)}
                                    </td>
                                )}
                            </tr>
                        ))}
                        <tr className="current-indicator-row">
                            <th className="p-0"></th>
                            {licenseTypes.map((licenseType) => {
                                if (
                                    (currentLicense === "Essential" || currentLicense === "None") &&
                                    licenseType === "Community"
                                ) {
                                    return (
                                        <td key="Essential" className="community current">
                                            current
                                        </td>
                                    );
                                }
                                return (
                                    <td
                                        key={licenseType}
                                        className={classNames(licenseType.toLowerCase(), {
                                            "current bg-faded-primary":
                                                currentLicense === licenseType ||
                                                (currentLicense === "Community" && licenseType === "Free") ||
                                                (currentLicense === "Enterprise" && licenseType === "Production"),
                                        })}
                                    >
                                        {(currentLicense === licenseType ||
                                            (currentLicense === "Community" && licenseType === "Free") ||
                                            (currentLicense === "Enterprise" && licenseType === "Production")) &&
                                            "current"}
                                    </td>
                                );
                            })}
                        </tr>
                    </tbody>
                </Table>
            </div>
            {currentLicense === "None" && (
                <div className="hstack gap-4 justify-content-center mt-4 flex-wrap">
                    <a
                        href={buyLink}
                        target="_blank"
                        color="primary"
                        className="btn btn-primary btn-lg rounded-pill px-4"
                    >
                        <Icon icon="license" margin="me-3" />
                        Get License
                    </a>
                </div>
            )}
        </>
    );
}

function formatAvailabilityValue(data: FeatureAvailabilityValueData, canBeEnabledInCloud?: boolean): ReactNode {
    const value = data.overwrittenValue ?? data.value;

    let formattedValue: ReactNode = value;

    if (value === true) {
        formattedValue = <Icon icon="check" margin="m-0" color="success" />;
    }
    if (value === false) {
        if (canBeEnabledInCloud) {
            const cloudOnDemandId = "cloud-on-demand-" + uniqueId();
            return (
                <OverlayTrigger
                    placement="top"
                    overlay={
                        <Tooltip id={cloudOnDemandId}>
                            You can enable this feature in RavenDB Cloud Portal or by contacting support.
                        </Tooltip>
                    }
                >
                    <div className="d-inline-block">
                        <Icon id={cloudOnDemandId} icon="upgrade-arrow" margin="m-0" color="success" />
                    </div>
                </OverlayTrigger>
            );
        } else {
            formattedValue = <Icon icon="cancel" margin="m-0" color="danger" />;
        }
    }
    if (value === Infinity) {
        formattedValue = <Icon icon="infinity" margin="m-0" />;
    }

    if (data.overwrittenValue == null) {
        return formattedValue;
    }

    const id = "overwritten-availability-value-" + uniqueId();

    return (
        <>
            <div className="overwritten-value">
                {formattedValue}
                <OverlayTrigger
                    overlay={<Tooltip id={id}>Default value for your license is {data.value.toString()}.</Tooltip>}
                >
                    <div className="d-inline-block">
                        <Icon id={id} icon="info" color="info" margin="m-0" />
                    </div>
                </OverlayTrigger>
            </div>
        </>
    );
}

function UpgradeLinkButton() {
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));
    const cloudPricingLink = "https://cloud.ravendb.net/pricing";

    if (isCloud) {
        return (
            <a href={cloudPricingLink} target="_blank" className="btn btn-cloud rounded-pill">
                <Icon icon="cloud" />
                Cloud pricing
            </a>
        );
    }

    return (
        <a href={appUrl.forAbout()} className="btn btn-primary rounded-pill">
            <Icon icon="license" />
            See full comparison
        </a>
    );
}

export default function FeatureAvailabilitySummaryWrapper({
    isUnlimited,
    data,
}: FeatureAvailabilitySummaryProps & { isUnlimited: boolean }) {
    const { value: isOpen, toggle: toggleIsOpen } = useBoolean(false);

    return (
        <>
            <div
                className={classNames("license-accordion panel-bg-1 accordion-item", {
                    "license-limited": !isUnlimited,
                })}
                onClick={toggleIsOpen}
            >
                <h2 className="accordion-header">
                    <button type="button" aria-expanded="true" className="accordion-button new-tab-button">
                        <i className="icon-license text-success me-1 tab-icon me-3 icon-md"></i>
                        <div className="vstack gap-1">
                            <div className="hstack flex-wrap gap-1">
                                <h4 className="m-0">Licensing</h4>
                            </div>
                            <small className="description">See which plans offer this and more exciting features</small>
                        </div>
                    </button>
                </h2>
            </div>
            {isOpen && <FeatureAvailabilitySummaryModal data={data} toggleIsOpen={toggleIsOpen} />}
        </>
    );
}

function FeatureAvailabilitySummaryModal({
    data,
    toggleIsOpen,
}: FeatureAvailabilitySummaryProps & { toggleIsOpen: () => void }) {
    const licenseType = useAppSelector(licenseSelectors.licenseType);

    return (
        <Modal size="lg" show onHide={toggleIsOpen} contentClassName="modal-border bulge-primary">
            <Modal.Header closeButton onCloseClick={toggleIsOpen}>
                <h3>
                    <Icon icon="license" />
                    License comparison
                </h3>
                {licenseType !== "Developer" && licenseType !== "EnterpriseAi" && (
                    <>
                        <br />
                        <div>
                            If you are developing you can test this and many more features using free{" "}
                            <a href="TODO kalczur" className="text-developer">
                                Developer license <Icon icon="newtab" margin="m-0" />
                            </a>
                        </div>
                    </>
                )}
            </Modal.Header>
            <Modal.Body>
                <FeatureAvailabilitySummary data={data} />
            </Modal.Body>
            <Modal.Footer className="hstack gap-2 justify-content-end">
                <Button variant="link" onClick={toggleIsOpen} className="link-muted">
                    Close
                </Button>
                {licenseType !== "EnterpriseAi" && licenseType !== "None" && <UpgradeLinkButton />}
            </Modal.Footer>
        </Modal>
    );
}
