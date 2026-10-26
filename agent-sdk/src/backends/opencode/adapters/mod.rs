use base64::Engine as _;

use crate::backends::opencode::protocol::responses::{
    OpenCodeNativeModel, OpenCodeProviderInventory,
};
use crate::{ModelInfo, ModelSelector, SdkError};

const MAX_CONNECTED_MODELS: usize = 65_536;
const PRIVATE_SELECTOR_VERSION: u8 = 1;

#[derive(Clone, Debug)]
pub(crate) struct OpenCodeModelInventory {
    models: Vec<OpenCodeSelectedModel>,
}

#[derive(Clone, Debug)]
pub(crate) struct OpenCodeSelectedModel {
    native: OpenCodeNativeModel,
    portable: ModelInfo,
}

impl OpenCodeModelInventory {
    pub(crate) fn from_native(providers: OpenCodeProviderInventory) -> Result<Self, SdkError> {
        let native_models = providers.connected_models()?;
        if native_models.len() > MAX_CONNECTED_MODELS {
            return Err(SdkError::integration(
                "connected model inventory exceeds its configured bound",
            ));
        }

        let mut models = native_models
            .into_iter()
            .map(OpenCodeSelectedModel::from_native)
            .collect::<Result<Vec<_>, _>>()?;
        models.sort_by(|left, right| left.selector().cmp(right.selector()));
        if models
            .windows(2)
            .any(|pair| pair[0].selector() == pair[1].selector())
        {
            return Err(SdkError::invalid_state(
                "connected model inventory contains duplicate identities",
            ));
        }
        Ok(Self { models })
    }

    pub(crate) fn portable(&self) -> Vec<ModelInfo> {
        self.models
            .iter()
            .map(|model| model.portable.clone())
            .collect()
    }

    pub(crate) fn select(
        &self,
        selector: &ModelSelector,
    ) -> Result<OpenCodeSelectedModel, SdkError> {
        self.models
            .binary_search_by(|candidate| candidate.selector().cmp(selector))
            .ok()
            .map(|index| self.models[index].clone())
            .ok_or_else(|| {
                SdkError::unsupported("selected model is not present in the connected inventory")
            })
    }
}

impl OpenCodeSelectedModel {
    fn from_native(native: OpenCodeNativeModel) -> Result<Self, SdkError> {
        let selector = Self::private_selector(&native)?;
        let display_name = match native.display_name() {
            "" => None,
            name if name.trim() == name => Some(name.to_owned()),
            _ => {
                return Err(SdkError::protocol("model display information is malformed"));
            }
        };
        let portable = ModelInfo::new(selector, display_name, Vec::new(), false)
            .map_err(|_| SdkError::protocol("model inventory is malformed"))?;
        Ok(Self { native, portable })
    }

    fn private_selector(native: &OpenCodeNativeModel) -> Result<ModelSelector, SdkError> {
        let provider = native.provider_id().as_bytes();
        let model = native.model_id().as_bytes();
        if provider.is_empty()
            || model.is_empty()
            || native.provider_id().trim() != native.provider_id()
            || native.model_id().trim() != native.model_id()
        {
            return Err(SdkError::protocol(
                "model inventory contains a malformed identity",
            ));
        }
        let provider_len = u32::try_from(provider.len())
            .map_err(|_| SdkError::integration("model identity exceeds its configured bound"))?;
        let mut encoded = Vec::with_capacity(
            1_usize
                .saturating_add(std::mem::size_of::<u32>())
                .saturating_add(provider.len())
                .saturating_add(model.len()),
        );
        encoded.push(PRIVATE_SELECTOR_VERSION);
        encoded.extend_from_slice(&provider_len.to_be_bytes());
        encoded.extend_from_slice(provider);
        encoded.extend_from_slice(model);
        ModelSelector::new(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(encoded))
            .map_err(|_| SdkError::invalid_state("private model selector is invalid"))
    }

    pub(crate) fn native(&self) -> &OpenCodeNativeModel {
        &self.native
    }

    pub(crate) fn selector(&self) -> &ModelSelector {
        self.portable.selector()
    }
}
